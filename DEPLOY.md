# RIO PRINT MEDIA — NEU — Deployment

## Live app
https://rio-print-media-test.onrender.com

## Source of truth
GitHub repo: https://github.com/rioprintmediaa/Rio-Print-Media-Test.git (branch: `main`)
Local source folder: `D:\Rio\Softwares\Merger\Test\Mongo\Rio-Print-Media-Test\neu`

## Deploying
Run `deploy_to_render_neu.bat` **from the folder containing your current
working files** (the batch file, `Rio_Sales_Tracker_ONLINE.html` or a versioned
variant like `Rio_Sales_Tracker_ONLINE_v39.html`, `rio_api.py`, etc. all live
together).

The script:
1. Creates (or reuses) a `Render\` subfolder next to the batch file.
2. Copies your current working files into `Render\`, renaming them to the
   canonical names the app expects (`Rio_Sales_Tracker_ONLINE.html`,
   `rio_api.py`) — this rename step is critical; skipping it is what silently
   breaks deploys.
3. Runs all git operations **inside `Render\`**, which is what actually gets
   force-pushed to `main`, which Render auto-deploys from.

Files pushed:
- `Rio_Sales_Tracker_ONLINE.html`
- `rio_api.py`
- `requirements.txt`
- `.gitignore`
- `DEPLOY.md`
- `VERSION`
- `CHANGELOG.md`

## Render service settings
- **Runtime:** Python 3
- **Build command:** `pip install -r requirements.txt`
- **Start command:** `uvicorn rio_api:app --host 0.0.0.0 --port $PORT`

## Required environment variables (set in Render dashboard, not in code)
| Variable | Purpose | Notes |
|---|---|---|
| `MONGO_URI` | MongoDB Atlas connection string | Falls back to the hardcoded test URI in `rio_api.py` if unset — **set this explicitly for anything beyond testing** |
| `MONGO_DB` | Database name | Defaults to `RioPrintMedia_Test` |
| `HTML_FILE` | HTML file the backend serves | Defaults to `Rio_Sales_Tracker_ONLINE.html` — should not normally need to change |

## Versioning
See `VERSION` and `CHANGELOG.md`. This deployment starts at **v1.0.0**; every
subsequent feature port or fix bumps the version and adds a `CHANGELOG.md` entry
before deploying.
