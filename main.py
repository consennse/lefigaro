def run_pipeline():
  import requests
  import xml.etree.ElementTree as ET
  import pandas as pd
  import json
  import zipfile
  import time
  import re

  # =========================================================
  # STEP 1 — BUILD scan.csv
  # =========================================================

  print("\n=== STEP 1: BUILD SCAN CSV ===")

  AGENCY_ID = "3374657"
  SOURCE_URL = "https://manda.propertybase.com/api/v2/feed/00DWx000007hlhBMAQ/XML2U/a0hSb000005gQ01IAE/full"

  RULE_FILE = "Poliris CSV Mapping.xlsx"
  MAP_FILE = "xml_map.json"

  CSV_NAME = "scan.csv"
  ZIP_NAME = f"{AGENCY_ID}.zip"
  DELIMITER = "!#"

  rules = pd.read_excel(RULE_FILE, header=9)
  rules.columns = rules.columns.str.strip()


  print("Loading Excel...")
  start = time.time()
  rules = pd.read_excel(RULE_FILE, header=9)
  print("Excel load time:", time.time() - start)

  FIELDS = []

  for _, r in rules.iterrows():

      if pd.isna(r["Rank"]):
          continue

      rank = int(r["Rank"])

      parent = str(r["Parent Node"]).replace("<", "").replace(">", "").strip()
      tag = str(r["Tag Name"]).replace("<", "").replace(">", "").strip()
      typ = str(r["Type"]).lower()

      xls_path = f"{parent}/{tag}" if tag else None

      # normalize type
      if "decimal" in typ:
          t = "decimal"
      elif "int" in typ:
          t = "int"
      elif "bool" in typ:
          t = "bool"
      else:
          t = "text"

      FIELDS.append((rank, xls_path, t))

  FIELDS = sorted(FIELDS, key=lambda x: x[0])

  print("Columns from XLS:", len(FIELDS))

  # ---------------- LOAD JSON MAP ----------------

  with open(MAP_FILE) as f:
      XML_MAP = json.load(f)

  # normalize JSON keys
  XML_MAP = {k.lower(): v for k, v in XML_MAP.items()}

  # ---------------- XML EXTRACT ----------------

  def extract(node, path):
      if not path:
          return ""

      try:
          current = node
          for part in path.split("/"):
              nxt = current.find(part)
              if nxt is None:
                  return ""
              current = nxt
          return current.text.strip() if current.text else ""
      except:
          return ""

  # ---------------- CLEANERS ----------------

  def clean_text(v):
      if not v:
          return ""
      v = v.replace('"', "'")
      v = v.replace("_x000D_", "<br>")    
      v = v.replace("\n", "")
      return v.strip()

  def to_decimal(v):
      try:
          num = float(v)
          if num == 0:
              return ""
          return f"{num:.2f}"
      except:
          return ""


  def to_int(v):
      try:
          num = int(float(v))
          if num == 0:
              return ""
          return str(num)
      except:
          return ""

  def to_bool(v):
      if not v:
          return ""
      return "OUI" if v.lower() in ["true", "1", "yes"] else "NON"

  def wrap(v):
      return f'"{v}"'

  # ---------------- RESOLVE RULE ----------------
  # ---------------- TRANSFORM RULES ----------------

  def transform(xls_path, value):
      key = xls_path.lower() if xls_path else ""

      # Type d'annonce
      if key == "general_listing_information/listingtype":
          if value.lower() == "sale":
              return "vente"
          return "location"

      # Furnished
      if key == "custom_fields/pba__Furnished_pb":
          return "OUI" if value else ""

      # Refurbished
      if key == "custom_fields/con_PolirisRefurbished":
          return "OUI" if value else ""
      
      if key == "custom_fields/con_elevator":
          return "OUI" if value else ""
      
      if key == "custom_fields/con_alarmsystem":
          return "OUI" if value else ""
      
      if key == "custom_fields/con_airconditioning":
          return "OUI" if value else ""
      
      if key == "custom_fields/pba__pool_pb":
          return "OUI" if value else ""
      
      if key == "custom_fields/con_wheelchairaccessible":
          return "OUI" if value else ""
      
      if key == "custom_fields/pba__fireplace_pb":
          return "OUI" if value else ""
      
      if key == "custom_fields/con_polirisworkneeded":
          return "OUI" if value else ""
      
      if key == "custom_fields/con_priceonrequest":
          return "OUI" if value else ""
      
      if key == "custom_fields/con_copro":
          return "OUI" if value else ""

      if key == "custom_fields/con_coproindifficulty":
          return "OUI" if value else ""
      if key == "custom_fields/con_polirisbuyerfee":
          return "0.0" 
      
    
      return value

  def resolve(listing, rule):
      if rule is None:
          return ""
      if isinstance(rule, str) and rule.startswith("DEFAULT:"):
          return rule.split("DEFAULT:")[1]
      return extract(listing, rule)

  # ---------------- DOWNLOAD XML ----------------
  # ---------------- DOWNLOAD XML ----------------

  print("Downloading XML...")

  with requests.get(SOURCE_URL, stream=True, timeout=60) as r:
      r.raise_for_status()
      with open("feed.xml", "wb") as f:
          total = 0
          for chunk in r.iter_content(1024 * 1024):  # 1MB
              if chunk:
                  f.write(chunk)
                  total += len(chunk)
                  print(f"Downloaded {total/1024/1024:.1f} MB", end="\r")

  print("\nParsing XML...")

  tree = ET.parse("feed.xml")
  root = tree.getroot()

  listings = root.findall(".//listing")
  print("Listings:", len(listings))

  # ---------------- BUILD CSV ----------------

  rows = []

  for listing in listings:
      row = [""] * 334

      for rank, xls_path, t in FIELDS:

          rule = XML_MAP.get(xls_path.strip().lower(), None)
          raw = resolve(listing, rule)
          raw = transform(xls_path, raw)

          if t == "decimal":
              value = to_decimal(raw)
              if xls_path.lower() == "custom_fields/con_polirisbuyerfee":
                  value = "0.0"
              else:
                  value = to_decimal(raw)

          elif t == "int":
              value = to_int(raw)
          elif t == "bool":
              if xls_path.lower() == "custom_fields/con_virtualtour":
                  value = clean_text(raw)
              else:
                  value = to_bool(raw)
          else:
              value = clean_text(raw)

          row[rank - 1] = wrap(value)


      # row = []
      
      # for rank, xls_path, t in FIELDS:

      #     rule = XML_MAP.get(xls_path.lower(), None)
      #     raw = resolve(listing, rule)
      #     raw = transform(xls_path, raw)

      #     if t == "decimal":
      #         value = to_decimal(raw)
      #     elif t == "int":
      #         value = to_int(raw)
      #     elif t == "bool":
      #         if xls_path.lower() == "custom_fields/con_virtualtour":
      #             value = clean_text(raw)   # keep URL
      #         else:
      #             value = to_bool(raw)

      #     else:
      #         value = clean_text(raw)

      #     row.append(wrap(value))

      # # ✅ copy column B → column FS
      # if len(row) > 174:
      #     row[174] = row[1]

      rows.append(row)

  # ---------------- WRITE CSV ----------------

  with open(CSV_NAME, "w", encoding="utf-8") as f:
      for r in rows:
          f.write(DELIMITER.join(r) + "\n")


  # =========================================================
  # STEP 2 — CSV → Excel
  # =========================================================

  print("\n=== STEP 2: CSV → EXCEL ===")

  df = pd.read_csv("scan.csv", sep="!#", engine="python", header=None)
  df = df.map(lambda x: x.strip('"') if isinstance(x,str) else x)
  df.to_excel("scan.xlsx", index=False, header=False)

  print("✅ scan.xlsx created")

  # =========================================================
  # STEP 3 — IMAGE EXTRACTION
  # =========================================================

  print("\n=== STEP 3: IMAGE EXTRACTION ===")

  IMAGE_COUNT = 30

  def extract_images(listing, limit=30):
      photos = []
      media = listing.find("listing_media")
      if media is not None:
          images = media.find("images")
          if images is not None:
              for img in images.findall("image"):
                  url = img.findtext("url","")
                  if url:
                      photos.append(url.strip())
      while len(photos) < limit:
          photos.append("")
      return photos[:limit]

  rows = []

  for listing in listings:
      row = [wrap(listing.findtext("id",""))]
      photos = extract_images(listing, IMAGE_COUNT)
      for p in photos:
          row.append(wrap(p))
      rows.append(row)

  with open("TEST.csv","w",encoding="utf-8") as f:
      for r in rows:
          f.write(DELIMITER.join(r) + "\n")

  print("✅ TEST.csv written")

  # =========================================================
  # STEP 4 — TEST → Excel
  # =========================================================

  df = pd.read_csv("TEST.csv", sep="!#", engine="python", header=None)
  df = df.map(lambda x: x.strip('"') if isinstance(x,str) else x)
  df.to_excel("TEST_ls.xlsx", index=False, header=False)

  print("✅ TEST_ls.xlsx created")

  # =========================================================
  # STEP 5 — MERGE
  # =========================================================


  SCAN_FILE = "scan.xlsx"
  TEST_FILE = "TEST_ls.xlsx"

  OUT_XLSX = "Annonces.xlsx"
  OUT_CSV = "Annonces.csv"

  DELIMITER = "!#"

  # ---------- CLEAN FUNCTIONS ----------

  def clean_id(v):
      if pd.isna(v):
          return ""
      v = str(v)
      v = v.replace('"', '')
      v = re.sub(r'\s+', '', v)
      return v.upper()  # normalize IDs only
  def clean(v):
      if pd.isna(v):
          return ""

      v = str(v).replace('"', '').strip()

      if v.lower() in ["nan", "none"]:
          return ""

      if re.fullmatch(r"\.\d+", v):
          return ""

      return v

  # ---------- LOAD FILES ----------

  scan = pd.read_excel(SCAN_FILE, header=None, dtype=str)
  test = pd.read_excel(TEST_FILE, header=None, dtype=str)

  # ✅ force numeric columns (kills .1 / .2 suffixes)
  scan.columns = range(scan.shape[1])
  test.columns = range(test.shape[1])

  # normalize ID columns
  scan[1] = scan[1].apply(clean_id)
  test[0] = test[0].apply(clean_id)

  # ---------- BUILD LOOKUP ----------

  lookup = {
      clean_id(row[0]): [clean(x) for x in row.tolist()]
      for _, row in test.iterrows()
      if clean_id(row[0])
  }

  print("LOOKUP SIZE:", len(lookup))

  # ---------- COLUMN MAP ----------

  column_map = {
      84:1, 85:2, 86:3, 87:4, 88:5, 89:6, 90:7, 91:8, 92:9,
      163:10, 164:11, 165:12, 166:13, 167:14, 168:15, 169:16, 170:17, 171:18, 
      172:19, 173:20, 263:21, 264:22, 265:23, 266:24, 267:25, 268:26, 269:27 , 270:28, 271:29, 272:30
  }

  # ensure scan wide enough
  max_cols = max(scan.shape[1], test.shape[1], 334)

  while scan.shape[1] < max_cols:
      scan[scan.shape[1]] = ""

  # ---------- MERGE ----------

  for i in range(len(scan)):

      key = scan.iat[i, 1]

      # blank mapped columns first
      for scan_col in column_map:
          scan.iat[i, scan_col] = ""

      if key not in lookup:
          continue

      test_row = lookup[key]

      for scan_col, test_col in column_map.items():
          if test_col < len(test_row):
              scan.iat[i, scan_col] = clean(test_row[test_col])

  # ---------- SAVE ----------

  scan.to_excel(OUT_XLSX, header=False, index=False)

  with open(OUT_CSV, "w", encoding="utf-8") as f:
      for _, row in scan.iterrows():
          f.write(DELIMITER.join(f'"{clean(x)}"' for x in row) + "\n")

  print("✅ merged.xlsx + merged.csv written")

  # =========================================================
  # FINAL ZIP
  # =========================================================

  with zipfile.ZipFile(ZIP_NAME, "w", zipfile.ZIP_DEFLATED) as z:

      # add final CSV
      z.write("Annonces.csv")

      # config.txt
      config_text = (
          "Version=4.12\r\n"
          "Application=Propertybase / 3.0\r\n"
          "Devise=Euro\r\n"
      )
      z.writestr("config.txt", config_text)

      # photos.cfg
      photos_text = "Mode=URL\r\n"
      z.writestr("photos.cfg", photos_text)

  print("✅ ZIP created with Annonces.csv + config + photos")

  # =========================================================
  from ftplib import FTP
  import time

  print("\n=== STEP 6: FTP UPLOAD (FIGARO PLAIN 21) ===")

  FTP_HOST = "ftp.figarocms.fr"
  FTP_USER = "tld-maisonvictoire"
  FTP_PASS = "Jvexn^bF%4"

  try:
    ftp = FTP()
    ftp.connect(FTP_HOST, 21, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)

    print("Connected to Figaro (plain FTP)")
    print("PWD:", ftp.pwd())
    print("FILES BEFORE:", ftp.nlst())

    with open(ZIP_NAME, "rb") as f:
        ftp.storbinary(f"STOR {ZIP_NAME}", f)

    print("FILES AFTER UPLOAD:", ftp.nlst())

    # Wait 5 seconds to see if server deletes it
    time.sleep(5)
    print("FILES 5 SECONDS LATER:", ftp.nlst())

    ftp.quit()

    print(f"✅ Upload attempt finished")

  except Exception as e:
    print("❌ FTP upload failed:", e)


if __name__ == "__main__":
    run_pipeline()
