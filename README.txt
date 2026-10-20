MALAYSIA TOP 50 - LIVE PRICES (delayed)
=======================================

Why the tab said "Could not reach the price feed"
  Browsers do not let a web page read Yahoo Finance directly (especially a file opened from your
  computer). So the prices are now fetched by a small Python script (yfinance, same as in Colab)
  and saved to prices.json. The dashboard reads that file and re-checks every price.

OPTION 1 - Google Colab (quickest, no setup, 2 minutes)
  1. Open colab.research.google.com > New notebook.
  2. In the first cell:   !pip -q install yfinance
  3. Click the folder icon (left side) > upload update_prices.py. In the next cell run:
        !python update_prices.py
     Your browser downloads prices.json and prices_live.xlsx (Excel copy). Read the line it prints, for example:
        Wrote prices.json: 50 fetched, 0 failed; 50 of 50 match the dashboard's 2026-10-02 close
     "50 of 50 match" proves the symbols and the data are right.
  4. Open the dashboard (Overview or Stock Analysis) > "Load prices file..." > choose prices.json > switch on "Use live prices".
  Optional: !python update_prices.py --inject "dashboard.html" also saves a copy of the dashboard with
  the prices stored inside, so anyone can open it and see them without loading a file.
  Re-run whenever you want fresh prices (Yahoo prices are delayed at least 15 minutes).

OPTION 2 - Automatic updates every 10 minutes (GitHub Pages, free)
  1. Create a GitHub repository and upload everything in this folder (keep the .github folder).
  2. Settings > Pages > Source: "Deploy from a branch" > main / root. Your link will be
        https://<your-name>.github.io/<repo>/
  3. Settings > Actions > General > Workflow permissions > "Read and write permissions" > Save.
  4. Actions tab > "Update prices" > Run workflow (first run; afterwards it runs by itself during
     Malaysian market hours). Each run commits a new prices.json; the page picks it up on every refresh.
  Notes: GitHub pauses scheduled workflows after 60 days without repository activity (press
  "Enable workflow"), and scheduled runs can be a few minutes late.

OPTION 3 - Your own computer
  pip install yfinance requests pandas openpyxl
  python update_prices.py
  then use "Load prices file..." in the dashboard.

How accuracy is protected
  - The Yahoo close for 2 Oct 2026 must equal the dashboard's close for each stock (checks symbol + source).
  - Symbol, currency (MYR), positive price and a quote time that is not in the future are verified, in the
    script and again in your browser.
  - Moves above 25% since 2 Oct are flagged. A newer reading is never replaced by an older one.
  - Yahoo's Bursa data is delayed and unofficial; it is not investment-grade real-time data.
