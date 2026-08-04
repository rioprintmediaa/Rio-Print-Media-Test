# Changelog — RIO ERP NEU

Versioning matches Rio's scheme: plain integers (v1, v2, v3...), not semantic
versioning. Every future change gets a new entry here and a bump in `VERSION`
before deploying.

## [v11] - Sales Tracker rewrite, security fix, button/card sizing
1. **Sales Tracker rewrite (the big one)** — replaced the fixed 2-row
   Size/Qty/Rate layout with v215's dynamic "+ Add Product" model:
   unlimited rows, add/remove any time, each row auto-calculates its own
   amount and folds into Total Amount with GST applied consistently.
   Updated every dependent function: `calcSizeAmt()`, `saveSalesEntry()`
   (now sends an `Items[]` array), `loadSalesEdit()` and `clearSalesForm()`
   (both work with dynamic rows), `onSalesProductChange()` (populates size
   dropdowns across all rows), and the invoice-generation function (now
   builds line items from `Items[]` instead of only ever reading the first
   two fixed rows). Kept `Size1`/`Size2`/`Qty1`/`Qty2`/`Rate1`/`Rate2` in
   sync with `Items[0]`/`[1]` for backward compatibility with older saved
   records and any other code still reading those directly.
   - Verified with a real calculation, not just a visual check: added 3
     rows (qty 2/4/6 × rate 100 = ₹1200 base), confirmed GST folds in
     correctly (₹1416 with 18%), then removed a row and confirmed the
     total recalculated correctly (₹1180) — add and remove both tested.
2. **Login security fix**: `/api/login` had a fallback that accepted ANY
   non-empty username/password combination as a valid admin login —
   anyone could type anything and get in. Removed entirely; only real
   `rio_users` records or a single named `admin` account (with an actual
   password check, not a blank check) can log in now.
3. **Card sizing standardized**: Purchase & Stock (`padding:24px 20px`)
   and Reports (`padding:22px`, `border-radius:14px`) cards now match
   Company Details' compact sizing (`padding:18px`, `border-radius:12px`)
   across all 18 cards in the three panels.
4. **Nav + action buttons lightened ~20%** (`#34363c` → `#63666f`) so
   they're clearly visible against the dark panel, and action buttons now
   use the exact same neumorphic shadow model as nav buttons instead of a
   larger/more diffuse shadow that was combining with the cyan border to
   create a "pit" look.
Verified: full sweep through all 21 panels, every Purchase & Stock and
Company Details sub-section, and the sales form's add/remove/save/edit
functions — zero JS console errors.

### Still pending confirmation
- Nav buttons → light grey (sample delivered, not yet a final decision)
- Button glow redesign (sample delivered, not yet a final decision)

## [v10] - Button/table/popup theming pass + 2 real bugs found
1. **Buttons**: white text, cyan icons, cyan outline border (referenced the
   Remaining Balance box's existing style as the pattern to match)
2. (combined with #1)
3. **Tables app-wide**: found the real bug — the global table system had
   **hardcoded light blue-grey zebra-striping** (`#e8ecf2`/`#d5dae4`) left
   over from before the dark theme, which is why tables still looked light
   everywhere. Fixed to dark theme-aware colors, added column border lines
   (previously only row lines existed)
4. **Headline colors preserved, other text fixed**: while checking this,
   found the P&L page's Monthly Breakdown and Expenses by Category tables
   were still using fully hardcoded light-theme styling from before dark
   mode existed (`#f5f5f5` header rows, `#333`/`#555` text, `white`/`#fafafa`
   row stripes) — completely invisible against the dark theme. Fixed both
   tables' headers and JS-rendered rows.
6. **Date pickers**: cyan calendar icon via `color-scheme:dark` + CSS filter
   on the picker indicator. (Caught and fixed a typo — accidental full-width
   characters in a filter value — before it shipped.)
7. **Popups unified**: audited every modal against the DB Sync popup's
   pattern (`var(--bg)` + neumorphic shadow). Found ~45 instances in my own
   Purchase & Stock/Contact modals where inputs had no background set
   (defaulting to browser white) and labels/borders were too light — fixed.
9. **Table headers**: cyan (or a distinct accent per table) to highlight,
   combined with the table fix above.
10. **Ledger "FY Opening Balances" collapsed box — real bug, fixed**:
    `.btn-row` had zero margin-top against the `.form-grid` above it,
    causing the button row to crowd/overlap the inputs. Verified with
    a before/after screenshot.
12. **Report-card boxes** (Reports/Purchase & Stock/Company Details tabs):
    were using `background:var(--bg)`, identical to the page background,
    which is why they "mingled" — gave them a distinct lighter shade
    (`#454850`) so they stand out as intended.

### Investigated, not done this pass
- **Item 5** (Sales Tracker "still old"): found the real explanation —
  v215's Order Entry form uses **dynamic, unlimited product line items**
  ("+ Add Product" button, one GST% per row), while Neu has a fixed 2-row
  layout. This is a genuine rewrite comparable in size to the Purchase &
  Stock build, not a styling fix — flagging as the next dedicated feature.
- **Items 8 & 11** (nav buttons → light grey; remove neumorphic glow,
  propose new button effect): both explicitly requested a sample before
  applying. Built and delivered two samples (`sample_nav_light_grey.png`,
  `sample_flat_button_style.png`) — not yet applied to the live file,
  pending confirmation.

Verified: full sweep through all 21 panels plus every Purchase & Stock and
Company Details sub-section, zero JS console errors.

## [v9] - Dark grey gradient theme (login + full dashboard)
- **Login page**: dark charcoal gradient background, login card scoped to
  its own dark CSS variables (all text/inputs auto-adapt), ECG "blood flow"
  line animation removed and replaced with soft blurred cyan/red glow blobs
  in the corners
- **Dashboard/app shell**: dark gradient applied to `#app-container`,
  scoped CSS variables (`--bg`, `--sd`, `--sl`, `--tp`, `--ts`) cascade the
  theme through every panel that already used `var(--bg)`/`var(--tp)` etc.
  (which turned out to be most of the app — confirms the CSS variable
  architecture was already solid)
- **Nav button glow fixed** (the specific complaint): `.nav-btn`,
  `.nav-footer-btn` (DB Sync/Sign Out), and the SALES/BILLING tab pills had
  a hardcoded light-theme neumorphic shadow (bright white highlight) that
  looked like a stray glow against the new dark nav panel. Replaced with a
  dark-appropriate neumorphic shadow (dark highlight + darker shadow, same
  raised/pressed effect, no white glow)
- **Found and fixed 27 hardcoded white card backgrounds** across Purchase &
  Stock and Company Details that don't use CSS variables — converted to
  `var(--bg)` so they follow the theme. Deliberately left the 3 invoice/
  quotation PDF print templates on plain white, since those represent
  actual printable paper documents and should stay white regardless of
  in-app theme
- **Fixed 3 low-contrast accent colors** that were dark shades meant for
  white cards (Admin & Partners, Bank Accounts, Login Logs, Stock Ledger)
  — brightened so titles/icons are readable against the new dark cards.
  This same fix incidentally improved the Reports panel's Customer-Wise
  card and several "Save PDF" buttons elsewhere, which shared the same
  color and the same underlying contrast problem
- **Caught and reverted a collateral hit**: a global color replacement
  briefly changed the generic reusable `.btn-action`/`.btn-action.secondary`
  pill-button component (used elsewhere, unrelated to Login Logs) —
  caught by checking every occurrence of the replaced color before
  finalizing, reverted those two specific instances
- Verified: full sweep through all 21 panels plus every Purchase & Stock
  and Company Details sub-section, zero JS console errors throughout

## [v8] - Text contrast, Eyelets feature (missing from v215 diff), darker secondary text
- **Darkened secondary text** app-wide: `--ts` (used for labels, captions,
  helper text throughout — login page "USERNAME"/"PASSWORD"/"Sign in to
  continue", panel section titles, etc.) changed from `#6c7293` to
  `#4a5072` for better readability
- **Eyelets feature ported (Rio-only)** — this was the one real gap found
  while diffing v213 vs v215 line-by-line (not just function names): v215
  changed how Eyelet Total folds into the sale's Total Amount (now added
  directly, no separate "Amt Inc. Eyelet" box), but Neu never had Eyelets
  at all. Added: Eyelet Qty/Rate/Total fields on the Sales Tracker form,
  GST applied at the same flat rate as the rest of the sale when "With
  Bill" is selected (adapted from v215's "first product row's GST%" since
  Neu uses a single flat rate rather than per-row GST%), folded directly
  into Total Amount per v215's latest behavior. Wired into save/edit/clear.
  Verified with an actual calculation: 10×₹50 size (with 18% GST) + 20×₹5
  eyelets (with 18% GST) = ₹708, matching the expected math exactly.
- Full v213→v215 diff came back small (241 lines) beyond the Expense Excel
  feature already ported in v7 — Eyelets was the only other real gap
- Verified: cycled through all 21 panels with zero JS console errors

## [v7] - UI consistency, critical sales bug fix, nav logic, v215 sync
1. **Purchase & Stock / Company Details header color fixed**: found that
   the Reports panel uses a neutral `var(--bg)` background with a colored
   left-border accent and colored title text — not a solid color block.
   Rewrote all 10 section headers (3 in Purchase & Stock, 7 in Company
   Details) to match that exact pattern, including the "← Back" button.
2. **Colored icon "images" → vector icons**: replaced all 10 colored
   square icon badges (52px in Purchase & Stock, 44px in Company Details)
   with flat colored SVG icons and no background box, matching the nav's
   existing minimalist icon style.
3. **Critical bug fix — sales entries not updating**: `add_sale()` only
   ever inserts records with an `Id` field, but `update_sale()`,
   `delete_sale()`, and the invoice-number endpoint were all querying by
   `SNo` — a field that was never actually set on insert. Every edit and
   delete was silently matching zero documents in MongoDB (no error, just
   a no-op), while the frontend still reported success. This is almost
   certainly what was meant by "entries are not updated like Rio v213."
   Fixed all three endpoints to match on `Id` OR `SNo`.
4. **Nav logic audit**: confirmed every nav button points to a real panel
   (no dead links). Found and fixed a real gap — P&L and Company Details
   panels only loaded their data on manual refresh/edit, unlike every
   other panel which loads fresh data automatically on nav click. Added
   both to `showPanel()`'s data-loading dispatch.
5. **Synced from Rio v215** (diffed against v213 — small, well-scoped
   delta): ported Expense Excel Template / Import / Export and an
   account filter for the expense list. Simplified vs. v215: skipped the
   ExcelJS cascading Category→Sub-Category dropdown validation (v215 uses
   a hardcoded category list; Neu loads categories dynamically from the
   backend), shipping a flat template with a "Valid Categories" reference
   sheet instead — still fully functional for bulk import. Added the
   SheetJS (XLSX) library.
- Verified: cycled through all 21 panels plus every Purchase & Stock and
  Company Details sub-section in a headless browser — zero JS console
  errors throughout.

## [v6] - Nav & Company Details UI fixes
- **Nav button consistency**: removed the special green gradient from the
  Profit/Loss and Purchase & Stock buttons — they now match every other nav
  button's plain style
- **Moved Purchase & Stock** from the Sales tab to the Billing tab, placed
  right after Account Ledger
- **Compacted the nav header**: reduced padding around the logo and the
  DASHBOARD block so more nav buttons fit on screen without scrolling —
  this is what was cutting off "SALES TRACKER" at the top of the nav list
  on narrower screens
- **Company Details now uses "box type tabs"**: converted from a long flat
  list of always-visible cards into the same card-grid + click-to-open
  pattern used by Purchase & Stock (7 cards: Admin & Partners, Bank
  Accounts, Factory Address, Investment Details, Generate Payment,
  Contacts, Login Logs)
- **FY selector label**: changed from "FY" to "Select FY" across all 16
  instances in the app
- Investigated "Sales Tracker page" issue — traced it to the same nav
  header overflow now fixed above; confirmed the customer dropdown was
  not touched and renders at its existing width
- Verified: cycled through all 21 panels in a headless browser with zero
  JS console errors; confirmed Company Details card navigation opens each
  section correctly

## [v5] - Company Details sub-sections (Phase 2, feature 4 of 5)
- **Bug fix**: Factory Address card was rendering outside `.panel-content`
  (a misplaced closing `</div>` cut the container short), so it didn't get
  the panel's padding/layout — fixed
- **Investment Details**: 3-stage itemized form (Proposal & Setup,
  Implementation, Kickoff), live per-stage subtotals + grand total, save/load
- **Generate Payment**: builds a UPI/GPay deep link from the Savings Account's
  UPI number already on file, plus a "Share via WhatsApp" button
- **Contacts**: phone-book style CRUD (Name, Phone, Description, Place,
  Category) with search and category filter
  - Intentionally simplified vs v213: no CSV import/export, no template
    download, no separate category-management modal — category is just a
    free-text field per contact
- **Login Logs**: every login attempt (success and failed) is now recorded
  with timestamp, username, result, and IP; viewable in a new Login Logs card
  - This required adding logging to `/api/login` itself, since Neu wasn't
    tracking login attempts at all before this
- Explicitly **not** ported: Renu Contacts and Debt & Loan — these are
  separate standalone apps with their own logins in v213
  (`Renu_Contacts_v213.html`, `Debt_and_Loan_v213.html`), not sub-sections of
  Company Details, and out of scope for this pass
- Verified: no JS console errors, all new DOM elements and functions
  confirmed present; modal nesting bug caught and fixed during verification
  (a first-draft edit had accidentally nested the new Contact modal inside
  the existing Company Edit modal)

### Still to come (Phase 2)
- Extra Reports sub-sections (Bank report, Breakdown report)

## [v4] - P&L confirmed + Purchase & Stock (Phase 2, features 2-3 of 5) + bug fix
- **Bug fix**: `/api/login` wasn't returning `username` in the user object,
  which meant the v3 follow-up reminder's `CreatedBy` field was always being
  saved blank — the user-specific filter silently matched nothing. Fixed.
- **P&L panel**: confirmed complete and verified. Uses Neu's already-loaded
  `salesData`/`expenseData` instead of separate API calls; GST is stripped
  from "With Bill" sales at a flat 18% since Neu's sales records don't carry
  a per-item GST breakdown the way v213's do.
- **Purchase & Stock panel** (new): Suppliers CRUD, Purchases (auto-writes
  Stock IN entries per line item), Stock Ledger (IN/OUT with running balance,
  manual OUT entry, filters by product/type/date). Backend: new `suppliers`,
  `purchases`, `stock_ledger` collections and endpoints.
  - Intentionally **not** ported: the Stock Mobile PIN access log (ties to
    `RIO_STOCK_MOBILE.html`, a separate mobile app Neu doesn't have) and the
    standalone Purchase Products catalog manager (replaced with a plain text
    product name field on purchase items)
  - No Rio/Rainbow company selector — Rio only, per earlier instruction
- Verified: both panels load with zero JS console errors, all new DOM
  elements and JS functions confirmed present via headless browser check

### Still to come (Phase 2)
- Extra Reports sub-sections (Bank report, Breakdown report)

## [v3] - Follow-up reminder popup (Phase 2, feature 1 of 5)
- Ported the follow-up reminder popup from v213: polls every 30s while
  logged in, shows a popup when a followup's Date+Time is due
- User-specific: only shows reminders created by the currently logged-in
  user (`CreatedBy`), not everyone's
- Snooze options (10 min / 1 hour / tomorrow) and Mark Addressed / Dismiss,
  same as v213
- Added `Time` field to the Add Followup form (was date-only before) and
  `CreatedBy` tracking on save
- Backend: added `PUT /api/followups/{id}/snooze`
- Verified: no JS console errors on load; reminder popup markup and all
  new JS functions confirmed present in the DOM

### Still to come (Phase 2)
- Company Details sub-sections (Investment Details, Bank Accounts,
  Admin & Partners, Login Logs, Generate Payment, Factory Address, Contacts)
- Extra Reports sub-sections (Bank report, Breakdown report)

## [v2] - Nav logo fix + dashboard cleanup
- Fixed logo/DASHBOARD button overlap: the Rio logo is now its own block above
  the nav (matches v213's layout — logo separate from the DASHBOARD nav
  button, which keeps its own icon)
- Removed the company name/address card from the Dashboard page (was
  duplicating the top header and looked out of place — flagged by user)

## [v1] - Phase 1: Nav & Dashboard branding
- Restored the Rio logo to the nav panel (top-left, above the DASHBOARD button)
- Added company name + address block to the Dashboard page (name, tagline,
  RSF address, phone, GST), ported from v213
- Confirmed nav layout: logo top-left, DASHBOARD button above the nav button list
- Explicitly excluded: CMY Color Correction / Layer Proof tools, and the
  "Proverb" login-screen widget — neither ported from v213

### Not yet ported (tracked for future versions)
- Follow-ups panel
- P&L panel
- Purchase & Stock panel (Purchases / Stock / Suppliers)
- Company Details sub-sections: Investment Details, Bank Accounts,
  Admin & Partners, Login Logs, Generate Payment, Factory Address, Contacts
- Extra Reports sub-sections: Bank report, Breakdown report
- Backend API parity with v213 (`rio_erp_api_v213.py` is ~4,700 lines vs
  Neu's `rio_api.py` at ~1,400 lines)
