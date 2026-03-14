"""
reports/order_app.py
Vamba Data — Order & Delivery Web Application

Routes:
  GET  /order                   — Order form
  POST /order/checkout          — Create Stripe Checkout Session → redirect
  GET  /order/success           — Post-payment: generate report, send email
  GET  /order/confirmation      — Confirmation page
  GET  /order/cancel            — Cancelled payment page
  GET  /admin/orders            — Admin order table (?password=xxx)
  POST /admin/orders/<id>/action — Admin actions (regenerate / resend / mark_delivered)

Run:
  python reports/order_app.py
  or via WSGI: gunicorn "reports.order_app:app"
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from flask import (
    Flask,
    redirect,
    render_template_string,
    request,
    url_for,
)
import stripe

# ── Bootstrap ─────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "configs" / ".env")

import sys
sys.path.insert(0, str(ROOT))

from config.launch_suburbs import get_all_available_suburbs, get_report_price, get_suburb_tier, REPORT_TYPES
from reports.report_builder import generate_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)

stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
APP_BASE_URL    = os.getenv("REPORT_APP_BASE_URL", "http://localhost:5055").rstrip("/")
ADMIN_PASSWORD  = os.getenv("ADMIN_PASSWORD", "changeme")

DB_PATH      = ROOT / "reports" / "orders.db"
REPORTS_DIR  = ROOT / "reports" / "output" / "paid_reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ── Database ──────────────────────────────────────────────────────────────────

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            report_id        TEXT PRIMARY KEY,
            stripe_session_id TEXT,
            suburb           TEXT NOT NULL,
            report_type      TEXT NOT NULL,
            bedrooms         INTEGER,
            budget_min       REAL,
            budget_max       REAL,
            buyer_email      TEXT NOT NULL,
            status           TEXT NOT NULL DEFAULT 'pending',
            created_at       TEXT NOT NULL,
            delivered_at     TEXT,
            pdf_path         TEXT
        )
    """)
    conn.commit()
    return conn


def _create_order(report_id, stripe_session_id, suburb, report_type,
                  bedrooms, budget_min, budget_max, buyer_email) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _db() as c:
        c.execute("""
            INSERT INTO orders
              (report_id, stripe_session_id, suburb, report_type,
               bedrooms, budget_min, budget_max, buyer_email,
               status, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (report_id, stripe_session_id, suburb, report_type,
              bedrooms, budget_min, budget_max, buyer_email, "pending", now))


def _update_order(report_id: str, **kwargs) -> None:
    allowed = {"status", "pdf_path", "delivered_at"}
    sets = ", ".join(f"{k} = ?" for k in kwargs if k in allowed)
    vals = [v for k, v in kwargs.items() if k in allowed]
    if not sets:
        return
    with _db() as c:
        c.execute(f"UPDATE orders SET {sets} WHERE report_id = ?", (*vals, report_id))


def _get_order(report_id: str) -> sqlite3.Row | None:
    with _db() as c:
        return c.execute("SELECT * FROM orders WHERE report_id = ?", (report_id,)).fetchone()


def _all_orders() -> list[sqlite3.Row]:
    with _db() as c:
        return c.execute("SELECT * FROM orders ORDER BY created_at DESC").fetchall()


# ── Email delivery ────────────────────────────────────────────────────────────

def _send_report_email(buyer_email: str, suburb: str, report_type: str,
                       pdf_path: str, report_id: str) -> bool:
    """
    Sends the completed report PDF to the buyer via SMTP.
    Returns True on success, False on failure (logs the error).
    """
    import smtplib
    from email.message import EmailMessage

    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    smtp_from = os.getenv("SMTP_FROM", smtp_user) or "hello@vambadata.com"

    if not smtp_host or not smtp_user or not smtp_pass:
        log.warning("SMTP not configured — skipping email send for order %s", report_id)
        return False

    current_date = date.today().strftime("%d %B %Y")
    type_label   = f"{report_type.title()} Properties"

    body = (
        f"Hi,\n\n"
        f"Your Zimbabwe property intelligence report for {suburb} is attached.\n\n"
        f"Report details:\n"
        f"  Suburb:      {suburb}\n"
        f"  Report type: {type_label}\n"
        f"  Report ID:   {report_id}\n\n"
        f"The report contains current asking price data sourced from 10+ Zimbabwe "
        f"property portals, verified as of {current_date}.\n\n"
        f"What to do next:\n"
        f"  1. Review the comparable listings section for properties similar to "
        f"what you are looking for\n"
        f"  2. Use the agent directory to contact agents who are active in this suburb\n"
        f"  3. Remember all prices are asking prices — budget for 5-15%% below "
        f"asking when making an offer\n\n"
        f"If you have questions about the report, reply to this email.\n\n"
        f"Vamba Data\n"
        f"hello@vambadata.com\n"
        f"vambadata.com"
    )

    msg = EmailMessage()
    msg["Subject"] = f"Your Vamba Data Report — {suburb} is ready"
    msg["From"]    = f"Vamba Data <{smtp_from}>"
    msg["To"]      = buyer_email
    msg.set_content(body)

    pdf_bytes = Path(pdf_path).read_bytes()
    msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf",
                       filename=f"VambaData_{suburb.replace(' ', '_')}_{report_type}.pdf")

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        log.info("Email sent to %s for order %s", buyer_email, report_id)
        return True
    except Exception as exc:
        log.error("Email send failed for order %s: %s", report_id, exc)
        return False


# ── Report generation + delivery ──────────────────────────────────────────────

def _generate_and_deliver(report_id: str) -> bool:
    """
    Generates the PDF and sends the email for a given order.
    Updates order status throughout. Returns True on full success.
    """
    order = _get_order(report_id)
    if not order:
        log.error("Order not found: %s", report_id)
        return False

    suburb      = order["suburb"]
    report_type = order["report_type"]
    bedrooms    = order["bedrooms"]
    budget_min  = order["budget_min"]
    budget_max  = order["budget_max"]
    buyer_email = order["buyer_email"]

    _update_order(report_id, status="generating")

    pdf_path = REPORTS_DIR / f"{report_id}_{suburb.replace(' ', '_').lower()}_{report_type}.pdf"

    try:
        generate_report(
            request={
                "suburb_name":  suburb,
                "listing_type": report_type,
                "bedrooms":     bedrooms,
                "budget_min":   budget_min,
                "budget_max":   budget_max,
                "buyer_email":  buyer_email,
                "report_id":    report_id,
            },
            output_path=str(pdf_path),
        )
        _update_order(report_id, pdf_path=str(pdf_path))
        log.info("PDF generated: %s", pdf_path)
    except Exception as exc:
        log.error("PDF generation failed for order %s: %s", report_id, exc)
        _update_order(report_id, status="failed")
        return False

    ok = _send_report_email(buyer_email, suburb, report_type, str(pdf_path), report_id)
    if ok:
        _update_order(report_id, status="delivered",
                      delivered_at=datetime.now(timezone.utc).isoformat())
        return True
    else:
        # PDF exists but email failed — mark generating so admin can resend
        _update_order(report_id, status="generating")
        return False


# ── HTML templates ────────────────────────────────────────────────────────────

_ORDER_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Vamba Data — Order Report</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: Arial, Helvetica, sans-serif; background: #f4f6f9; color: #222; }

    /* Header */
    .header { background: #0D1B3E; padding: 16px 24px; display: flex;
               align-items: center; justify-content: space-between; }
    .header-logo { color: #fff; font-size: 22px; font-weight: bold; }
    .header-tag  { color: #00BCD4; font-size: 13px; }
    .header-rule { border: none; border-top: 2px solid #00BCD4; }

    /* Hero */
    .hero { background: #fff; padding: 40px 24px 28px; text-align: center;
            border-bottom: 1px solid #e0e0e0; }
    .hero h1 { font-size: 28px; color: #0D1B3E; line-height: 1.3; max-width: 560px;
                margin: 0 auto 12px; }
    .hero p   { font-size: 14px; color: #666; max-width: 480px; margin: 0 auto 24px; }
    .trust-row { display: flex; justify-content: center; gap: 12px; flex-wrap: wrap; }
    .trust-box { background: #f0f8ff; border: 1px solid #b3d9f2; border-radius: 6px;
                  padding: 8px 14px; font-size: 12px; color: #0D1B3E; font-weight: bold; }

    /* Form card */
    .card-wrap { display: flex; justify-content: center; padding: 32px 16px 48px; }
    .card { background: #fff; border-radius: 8px; box-shadow: 0 2px 12px rgba(0,0,0,.1);
             padding: 32px; width: 100%%; max-width: 600px; }
    .card h2 { font-size: 20px; color: #0D1B3E; margin-bottom: 20px;
                border-bottom: 2px solid #00BCD4; padding-bottom: 10px; }

    label  { display: block; font-size: 13px; font-weight: bold; color: #333;
              margin-bottom: 5px; margin-top: 16px; }
    select, input[type=email], input[type=number] {
      width: 100%%; padding: 9px 12px; border: 1px solid #ccc; border-radius: 5px;
      font-size: 14px; color: #222; }
    select:focus, input:focus { outline: 2px solid #00BCD4; border-color: #00BCD4; }

    .radio-row { display: flex; gap: 24px; margin-top: 6px; }
    .radio-row label { font-weight: normal; display: flex; align-items: center; gap: 6px;
                        margin-top: 0; cursor: pointer; }

    .budget-row { display: flex; gap: 12px; }
    .budget-row > div { flex: 1; }

    #budget-section { margin-top: 0; }

    .price-display { margin-top: 16px; padding: 10px 14px; background: #f0f8ff;
                      border-left: 4px solid #00BCD4; font-size: 15px; color: #0D1B3E; }
    .price-display span { font-weight: bold; font-size: 18px; }

    .btn-order { display: block; width: 100%%; margin-top: 20px; padding: 14px;
                  background: #00BCD4; color: #fff; border: none; border-radius: 6px;
                  font-size: 16px; font-weight: bold; cursor: pointer; text-align: center; }
    .btn-order:hover { background: #0097a7; }

    .below-btn { margin-top: 12px; font-size: 12px; color: #888; text-align: center;
                  line-height: 1.8; }

    /* Footer */
    .footer { background: #0D1B3E; color: #aaa; font-size: 11px; text-align: center;
               padding: 16px; margin-top: 0; }
  </style>
</head>
<body>

<div class="header">
  <span class="header-logo">Vamba Data</span>
  <span class="header-tag">Zimbabwe Property Market Intelligence</span>
</div>
<hr class="header-rule"/>

<div class="hero">
  <h1>Know What Zimbabwe Property Is Really Worth</h1>
  <p>Real-time asking price data across 35+ Harare suburbs — delivered as a professional PDF report within 24 hours</p>
  <div class="trust-row">
    <div class="trust-box">35+ suburbs covered</div>
    <div class="trust-box">10+ data sources</div>
    <div class="trust-box">Verified {{ today }} data</div>
  </div>
</div>

<div class="card-wrap">
  <div class="card">
    <h2>Order Your Report</h2>
    <form method="post" action="/order/checkout" id="orderForm">

      <label>Report Type</label>
      <div class="radio-row">
        <label><input type="radio" name="report_type" value="sale" checked
               onchange="onTypeChange()"> For Sale Properties</label>
        <label><input type="radio" name="report_type" value="rent"
               onchange="onTypeChange()"> Rental Properties</label>
      </div>

      <label for="suburb">Suburb</label>
      <select name="suburb" id="suburb" onchange="onSuburbChange()">
        <option value="">— Select a suburb —</option>
        <optgroup label="Premium Suburbs ($49)">
          {% for s in tier1 %}
          <option value="{{ s.name }}" data-price="{{ s.price }}">{{ s.name }}</option>
          {% endfor %}
        </optgroup>
        <optgroup label="Standard Suburbs ($35)">
          {% for s in tier2 %}
          <option value="{{ s.name }}" data-price="{{ s.price }}">{{ s.name }}</option>
          {% endfor %}
        </optgroup>
      </select>

      <label for="bedrooms">Bedrooms</label>
      <select name="bedrooms" id="bedrooms">
        <option value="">Any</option>
        <option value="1">1 bedroom</option>
        <option value="2">2 bedrooms</option>
        <option value="3">3 bedrooms</option>
        <option value="4">4 bedrooms</option>
        <option value="5">5+ bedrooms</option>
      </select>

      <div id="budget-section">
        <label>Budget Range (optional, For Sale only)</label>
        <div class="budget-row">
          <div>
            <label style="font-weight:normal;font-size:12px;">Min $</label>
            <input type="number" name="budget_min" id="budget_min"
                   placeholder="e.g. 100000" min="0" step="1000"/>
          </div>
          <div>
            <label style="font-weight:normal;font-size:12px;">Max $</label>
            <input type="number" name="budget_max" id="budget_max"
                   placeholder="e.g. 500000" min="0" step="1000"/>
          </div>
        </div>
      </div>

      <label for="email">Your Email Address</label>
      <input type="email" name="email" id="email" required
             placeholder="you@example.com"/>

      <div class="price-display" id="priceDisplay" style="display:none;">
        Report price: <span id="priceAmt">$49</span>
      </div>

      <button type="submit" class="btn-order" id="submitBtn">
        Order Report
      </button>
    </form>

    <div class="below-btn">
      Your report will be delivered to your email within 24 hours.<br/>
      Secure payment via Stripe.<br/>
      Questions? Email <a href="mailto:hello@vambadata.com">hello@vambadata.com</a>
    </div>
  </div>
</div>

<div class="footer">
  &copy; 2026 Vamba Data &nbsp;|&nbsp;
  Data sourced from public Zimbabwean property portals &nbsp;|&nbsp;
  Not financial advice
</div>

<script>
  var prices = {{ prices_json }};

  function onSuburbChange() {
    var sel = document.getElementById('suburb');
    var opt = sel.options[sel.selectedIndex];
    var price = opt.getAttribute('data-price');
    var display = document.getElementById('priceDisplay');
    var amt     = document.getElementById('priceAmt');
    var btn     = document.getElementById('submitBtn');
    if (price) {
      display.style.display = 'block';
      amt.textContent = '$' + price;
      btn.textContent = 'Order Report — $' + price;
    } else {
      display.style.display = 'none';
      btn.textContent = 'Order Report';
    }
  }

  function onTypeChange() {
    var rtype = document.querySelector('input[name=report_type]:checked').value;
    var budgetSection = document.getElementById('budget-section');
    budgetSection.style.display = (rtype === 'sale') ? 'block' : 'none';
  }

  // Init
  onTypeChange();
</script>
</body>
</html>"""


_CONFIRMATION_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Order Confirmed — Vamba Data</title>
  <style>
    body { font-family: Arial, sans-serif; background: #f4f6f9; color: #222; }
    .header { background: #0D1B3E; padding: 16px 24px; }
    .header-logo { color: #fff; font-size: 22px; font-weight: bold; }
    .hr { border: none; border-top: 2px solid #00BCD4; }
    .wrap { max-width: 560px; margin: 40px auto; background: #fff;
             border-radius: 8px; box-shadow: 0 2px 12px rgba(0,0,0,.1); padding: 32px; }
    h2   { color: #0D1B3E; margin-bottom: 16px; }
    .badge { background: #e8f5e9; border: 1px solid #a5d6a7; border-radius: 6px;
              padding: 10px 16px; margin-bottom: 20px; color: #2e7d32; font-weight: bold; }
    table { width: 100%%; border-collapse: collapse; margin-bottom: 20px; }
    td { padding: 8px 12px; border-bottom: 1px solid #eee; font-size: 14px; }
    td:first-child { color: #666; width: 40%%; }
    .tips { background: #f0f4f8; border-left: 4px solid #0D1B3E;
             padding: 14px 18px; border-radius: 4px; margin-top: 16px; }
    .tips h3 { font-size: 13px; color: #0D1B3E; margin-bottom: 10px; }
    .tips li  { font-size: 13px; color: #333; margin-bottom: 6px; margin-left: 16px; }
    .footer { text-align: center; font-size: 11px; color: #aaa; margin-top: 32px; }
  </style>
</head>
<body>
<div class="header"><span class="header-logo">Vamba Data</span></div>
<hr class="hr"/>
<div class="wrap">
  <div class="badge">&#10003; Your report is being prepared</div>
  <h2>Order Confirmation</h2>
  <table>
    <tr><td>Suburb</td><td><b>{{ suburb }}</b></td></tr>
    <tr><td>Report type</td><td>{{ type_label }}</td></tr>
    {% if bedrooms %}<tr><td>Bedrooms</td><td>{{ bedrooms }}</td></tr>{% endif %}
    {% if budget_min or budget_max %}
    <tr><td>Budget range</td><td>
      {% if budget_min %}${{ "{:,.0f}".format(budget_min) }}{% else %}—{% endif %}
      &nbsp;–&nbsp;
      {% if budget_max %}${{ "{:,.0f}".format(budget_max) }}{% else %}—{% endif %}
    </td></tr>
    {% endif %}
    <tr><td>Delivery email</td><td>{{ email }}</td></tr>
    <tr><td>Report ID</td><td style="font-size:11px;color:#888;">{{ report_id }}</td></tr>
  </table>
  <p style="font-size:14px; color:#444;">
    You will receive an email at <b>{{ email }}</b> within 24 hours with your report attached.
  </p>

  <div class="tips">
    <h3>While you wait — three tips for buying property in Zimbabwe</h3>
    <ul>
      <li>All advertised prices are <i>asking prices</i>. Actual transaction prices are
          not publicly disclosed in Zimbabwe — budget for 5–15% negotiation room.</li>
      <li>Verify title deeds and rates clearance certificates through a registered
          conveyancer before committing to any property.</li>
      <li>USD cash transactions are the market norm for residential property in Harare.
          Confirm payment terms with the seller's agent before viewing.</li>
    </ul>
  </div>
  <div class="footer">Vamba Data &nbsp;|&nbsp; hello@vambadata.com &nbsp;|&nbsp; vambadata.com</div>
</div>
</body>
</html>"""


_CANCEL_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Order Cancelled — Vamba Data</title>
  <style>
    body { font-family: Arial, sans-serif; background: #f4f6f9; color: #222; }
    .header { background: #0D1B3E; padding: 16px 24px; }
    .header-logo { color: #fff; font-size: 22px; font-weight: bold; }
    .hr { border: none; border-top: 2px solid #00BCD4; }
    .wrap { max-width: 500px; margin: 60px auto; background: #fff;
             border-radius: 8px; box-shadow: 0 2px 12px rgba(0,0,0,.1);
             padding: 40px; text-align: center; }
    h2 { color: #0D1B3E; margin-bottom: 12px; }
    p  { color: #555; font-size: 14px; margin-bottom: 20px; }
    a.btn { display: inline-block; padding: 10px 24px; background: #00BCD4;
             color: #fff; text-decoration: none; border-radius: 5px; font-weight: bold; }
  </style>
</head>
<body>
<div class="header"><span class="header-logo">Vamba Data</span></div>
<hr class="hr"/>
<div class="wrap">
  <h2>Order Cancelled</h2>
  <p>Your order was cancelled. No charge has been made.</p>
  <a class="btn" href="/order">Back to Order Form</a>
</div>
</body>
</html>"""


_ADMIN_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Admin — Vamba Data Orders</title>
  <style>
    body { font-family: Arial, sans-serif; background: #f4f6f9; font-size: 13px; }
    .header { background: #0D1B3E; padding: 12px 20px; color: #fff; font-size: 18px; font-weight: bold; }
    .hr { border: none; border-top: 2px solid #00BCD4; }
    .wrap { padding: 20px; }
    h2 { color: #0D1B3E; margin-bottom: 16px; }
    .stats-row { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }
    .stat-box { background: #fff; border-radius: 6px; padding: 12px 20px;
                 box-shadow: 0 1px 4px rgba(0,0,0,.1); min-width: 140px; }
    .stat-box .val { font-size: 22px; font-weight: bold; color: #0D1B3E; }
    .stat-box .lbl { font-size: 11px; color: #888; margin-top: 2px; }
    table { width: 100%%; border-collapse: collapse; background: #fff;
             border-radius: 6px; overflow: hidden;
             box-shadow: 0 1px 4px rgba(0,0,0,.1); }
    th { background: #0D1B3E; color: #fff; padding: 10px 8px; text-align: left; }
    td { padding: 9px 8px; border-bottom: 1px solid #eee; vertical-align: middle; }
    tr:last-child td { border-bottom: none; }
    tr:nth-child(even) td { background: #fafafa; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: bold; }
    .badge-pending    { background: #fff3e0; color: #e65100; }
    .badge-generating { background: #e3f2fd; color: #0277bd; }
    .badge-delivered  { background: #e8f5e9; color: #2e7d32; }
    .badge-failed     { background: #ffebee; color: #c62828; }
    .btn-sm { padding: 4px 10px; font-size: 11px; border: none; border-radius: 4px;
               cursor: pointer; margin-right: 4px; }
    .btn-regen  { background: #0D1B3E; color: #fff; }
    .btn-resend { background: #00BCD4; color: #fff; }
    .btn-mark   { background: #2e7d32; color: #fff; }
    .flash { background: #e8f5e9; border: 1px solid #a5d6a7; border-radius: 4px;
              padding: 8px 14px; margin-bottom: 16px; color: #2e7d32; font-weight: bold; }
    .flash-err { background: #ffebee; border-color: #ef9a9a; color: #c62828; }
  </style>
</head>
<body>
<div class="header">Vamba Data — Admin</div>
<hr class="hr"/>
<div class="wrap">
  <h2>Order Management</h2>
  {% if flash %}<div class="flash">{{ flash }}</div>{% endif %}
  {% if flash_err %}<div class="flash flash-err">{{ flash_err }}</div>{% endif %}

  <div class="stats-row">
    <div class="stat-box">
      <div class="val">{{ total_orders }}</div>
      <div class="lbl">Total orders</div>
    </div>
    <div class="stat-box">
      <div class="val">${{ total_revenue }}</div>
      <div class="lbl">Revenue (delivered)</div>
    </div>
    <div class="stat-box">
      <div class="val">{{ counts.delivered }}</div>
      <div class="lbl">Delivered</div>
    </div>
    <div class="stat-box">
      <div class="val">{{ counts.pending + counts.generating }}</div>
      <div class="lbl">In progress</div>
    </div>
    <div class="stat-box">
      <div class="val">{{ counts.failed }}</div>
      <div class="lbl">Failed</div>
    </div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Report ID</th>
        <th>Suburb</th>
        <th>Type</th>
        <th>Email</th>
        <th>Status</th>
        <th>Created (UTC)</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody>
    {% for o in orders %}
      <tr>
        <td style="font-size:10px;color:#888;">{{ o.report_id[:16] }}…</td>
        <td><b>{{ o.suburb }}</b></td>
        <td>{{ o.report_type }}</td>
        <td>{{ o.buyer_email }}</td>
        <td><span class="badge badge-{{ o.status }}">{{ o.status }}</span></td>
        <td>{{ o.created_at[:16] }}</td>
        <td>
          <form method="post" action="/admin/orders/{{ o.report_id }}/action?password={{ pwd }}" style="display:inline;">
            <button name="action" value="regenerate" class="btn-sm btn-regen">Regenerate</button>
            <button name="action" value="resend" class="btn-sm btn-resend">Resend Email</button>
            <button name="action" value="mark_delivered" class="btn-sm btn-mark">Mark Delivered</button>
          </form>
        </td>
      </tr>
    {% else %}
      <tr><td colspan="7" style="text-align:center;color:#aaa;padding:24px;">No orders yet.</td></tr>
    {% endfor %}
    </tbody>
  </table>
</div>
</body>
</html>"""


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/order")
def order_form():
    suburbs  = get_all_available_suburbs()
    tier1    = [s for s in suburbs if s["tier"] == 1]
    tier2    = [s for s in suburbs if s["tier"] == 2]
    prices   = {s["name"]: s["price"] for s in suburbs}
    import json
    return render_template_string(
        _ORDER_HTML,
        tier1=tier1,
        tier2=tier2,
        prices_json=json.dumps(prices),
        today=date.today().strftime("March %Y"),
    )


@app.post("/order/checkout")
def order_checkout():
    suburb      = (request.form.get("suburb") or "").strip()
    report_type = (request.form.get("report_type") or "sale").strip()
    bedrooms_s  = (request.form.get("bedrooms") or "").strip()
    budget_min_s = (request.form.get("budget_min") or "").strip()
    budget_max_s = (request.form.get("budget_max") or "").strip()
    email       = (request.form.get("email") or "").strip()

    errors = []
    if not suburb or get_suburb_tier(suburb) == 0:
        errors.append("Please select a valid suburb.")
    if report_type not in REPORT_TYPES:
        errors.append("Invalid report type.")
    if not email or not _EMAIL_RE.match(email):
        errors.append("Please enter a valid email address.")

    bedrooms  = int(bedrooms_s) if bedrooms_s.isdigit() else None
    try:
        budget_min = float(budget_min_s) if budget_min_s else None
        budget_max = float(budget_max_s) if budget_max_s else None
    except ValueError:
        budget_min = budget_max = None

    if errors:
        return "<br>".join(errors) + '<br><a href="/order">Back</a>', 400

    if not stripe.api_key:
        return "Stripe is not configured. Contact hello@vambadata.com.", 500

    price_cents = get_report_price(suburb) * 100
    report_id   = str(uuid.uuid4())

    session = stripe.checkout.Session.create(
        mode="payment",
        success_url=f"{APP_BASE_URL}/order/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{APP_BASE_URL}/order/cancel",
        customer_email=email,
        metadata={
            "report_id":  report_id,
            "suburb":     suburb,
            "report_type": report_type,
            "bedrooms":   str(bedrooms) if bedrooms is not None else "",
            "budget_min": str(budget_min) if budget_min is not None else "",
            "budget_max": str(budget_max) if budget_max is not None else "",
            "email":      email,
        },
        line_items=[{
            "price_data": {
                "currency": "usd",
                "product_data": {
                    "name": f"Vamba Data Report — {suburb} ({report_type.title()})",
                },
                "unit_amount": price_cents,
            },
            "quantity": 1,
        }],
    )

    _create_order(report_id, session.id, suburb, report_type,
                  bedrooms, budget_min, budget_max, email)
    log.info("Checkout session created for order %s (%s %s)", report_id, suburb, report_type)
    return redirect(session.url, code=303)


@app.get("/order/success")
def order_success():
    session_id = request.args.get("session_id", "")
    if not session_id or not stripe.api_key:
        return redirect(url_for("order_form"))

    try:
        session = stripe.checkout.Session.retrieve(session_id)
    except Exception as exc:
        log.error("Failed to retrieve Stripe session %s: %s", session_id, exc)
        return redirect(url_for("order_form"))

    md = session.get("metadata", {})
    report_id   = md.get("report_id", str(uuid.uuid4()))
    suburb      = md.get("suburb", "")
    report_type = md.get("report_type", "sale")
    email       = md.get("email", "")
    bedrooms_s  = md.get("bedrooms", "")
    budget_min_s = md.get("budget_min", "")
    budget_max_s = md.get("budget_max", "")

    bedrooms  = int(bedrooms_s) if bedrooms_s.isdigit() else None
    try:
        budget_min = float(budget_min_s) if budget_min_s else None
        budget_max = float(budget_max_s) if budget_max_s else None
    except ValueError:
        budget_min = budget_max = None

    existing = _get_order(report_id)
    if not existing:
        _create_order(report_id, session_id, suburb, report_type,
                      bedrooms, budget_min, budget_max, email)
    else:
        _update_order(report_id, status="pending")

    log.info("Payment confirmed for order %s — starting generation", report_id)
    _generate_and_deliver(report_id)
    return redirect(url_for("order_confirmation", report_id=report_id))


@app.get("/order/confirmation")
def order_confirmation():
    report_id = request.args.get("report_id", "")
    order = _get_order(report_id) if report_id else None
    if not order:
        return redirect(url_for("order_form"))

    type_label = "For Sale Properties" if order["report_type"] == "sale" else "Rental Properties"
    return render_template_string(
        _CONFIRMATION_HTML,
        suburb=order["suburb"],
        type_label=type_label,
        bedrooms=order["bedrooms"],
        budget_min=order["budget_min"],
        budget_max=order["budget_max"],
        email=order["buyer_email"],
        report_id=report_id,
    )


@app.get("/order/cancel")
def order_cancel():
    return render_template_string(_CANCEL_HTML)


@app.get("/admin/orders")
def admin_orders():
    pwd = request.args.get("password", "")
    if pwd != ADMIN_PASSWORD:
        return "Unauthorized. Add ?password=your_admin_password to the URL.", 403

    flash     = request.args.get("flash", "")
    flash_err = request.args.get("flash_err", "")

    orders    = _all_orders()
    total     = len(orders)
    counts    = {"pending": 0, "generating": 0, "delivered": 0, "failed": 0}
    revenue   = 0
    for o in orders:
        s = o["status"]
        if s in counts:
            counts[s] += 1
        if s == "delivered":
            revenue += get_report_price(o["suburb"])

    return render_template_string(
        _ADMIN_HTML,
        orders=orders,
        total_orders=total,
        total_revenue=revenue,
        counts=counts,
        pwd=pwd,
        flash=flash,
        flash_err=flash_err,
    )


@app.post("/admin/orders/<report_id>/action")
def admin_action(report_id: str):
    pwd = request.args.get("password", "")
    if pwd != ADMIN_PASSWORD:
        return "Unauthorized.", 403

    action = request.form.get("action", "")
    order  = _get_order(report_id)
    if not order:
        return redirect(url_for("admin_orders", password=pwd, flash_err="Order not found."))

    if action == "regenerate":
        ok = _generate_and_deliver(report_id)
        msg = f"Regenerated and {'delivered' if ok else 'generation failed — check logs'}."
        param = "flash" if ok else "flash_err"
    elif action == "resend":
        pdf = order["pdf_path"]
        if not pdf or not Path(pdf).exists():
            return redirect(url_for("admin_orders", password=pwd,
                                    flash_err="No PDF found — regenerate first."))
        ok = _send_report_email(order["buyer_email"], order["suburb"],
                                 order["report_type"], pdf, report_id)
        if ok:
            _update_order(report_id, status="delivered",
                          delivered_at=datetime.now(timezone.utc).isoformat())
        msg   = "Email resent." if ok else "Email send failed — check SMTP config."
        param = "flash" if ok else "flash_err"
    elif action == "mark_delivered":
        _update_order(report_id, status="delivered",
                      delivered_at=datetime.now(timezone.utc).isoformat())
        msg, param = "Marked as delivered.", "flash"
    else:
        msg, param = "Unknown action.", "flash_err"

    return redirect(url_for("admin_orders", password=pwd, **{param: msg}))


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("REPORT_APP_PORT", "5055"))
    log.info("Starting Vamba Data order app on port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False)
