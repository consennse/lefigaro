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

  FIELDS = []

  for _, r in rules.iterrows():
      if pd.isna(r["Rank"]):
          continue

      rank = int(r["Rank"])
      parent = str(r["Parent Node"]).replace("<", "").replace(">", "").strip()
      tag = str(r["Tag Name"]).replace("<", "").replace(">", "").strip()
      typ = str(r["Type"]).lower()

      xls_path = f"{parent}/{tag}" if tag else None

      if "decimal" in typ: t = "decimal"
      elif "int" in typ: t = "int"
      elif "bool" in typ: t = "bool"
      else: t = "text"

      FIELDS.append((rank, xls_path, t))

  FIELDS = sorted(FIELDS, key=lambda x: x[0])

  with open(MAP_FILE) as f:
      XML_MAP = {k.lower(): v for k, v in json.load(f).items()}

  def extract(node, path):
      if not path: return ""
      try:
          current = node
          for part in path.split("/"):
              current = current.find(part)
              if current is None:
                  return ""
          return current.text.strip() if current.text else ""
      except:
          return ""

  def clean_text(v):
      if not v: return ""
      return v.replace('"', "'").replace("_x000D_", "<br>").replace("\n", "").strip()

  def to_decimal(v):
      try:
          num = float(v)
          if num == 0: return ""
          return f"{num:.2f}"
      except:
          return ""

  def to_int(v):
      try:
          num = int(float(v))
          if num == 0: return ""
          return str(num)
      except:
          return ""

  def to_bool(v):
      if not v: return ""
      return "OUI" if v.lower() in ["true","1","yes"] else "NON"

  def wrap(v):
      return f'"{v}"'

  print("Downloading XML...")
  r = requests.get(SOURCE_URL)
  root = ET.fromstring(r.content)
  listings = root.findall(".//listing")

  rows = []

  for listing in listings:
      row = [""] * 334

      for rank, xls_path, t in FIELDS:
          rule = XML_MAP.get(xls_path.lower(), None)
          raw = extract(listing, rule)

          if t == "decimal":
              value = to_decimal(raw)
          elif t == "int":
              value = to_int(raw)
          elif t == "bool":
              value = to_bool(raw)
          else:
              value = clean_text(raw)

          row[rank-1] = wrap(value)

      rows.append(row)

  with open(CSV_NAME, "w", encoding="utf-8") as f:
      for r in rows:
          f.write(DELIMITER.join(r) + "\n")


  print("✅ scan.csv written")

  # =========================================================
  # STEP 2 — CSV → Excel
  # =========================================================

  print("\n=== STEP 2: CSV → EXCEL ===")

  df = pd.read_csv("scan.csv", sep="!#", engine="python", header=None)
  df = df.map(lambda x: x.strip('"') if isinstance(x,str) else x)
  df.to_excel("ls.xlsx", index=False, header=False)

  print("✅ ls.xlsx created")

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

  print("\n=== STEP 5: MERGE ===")

  def clean_id(v):
      if pd.isna(v): return ""
      return re.sub(r"\s+","",str(v).replace('"','')).upper()

  def clean(v):
      if pd.isna(v): return ""
      v = str(v).replace('"','').strip()
      if v.lower() in ["nan","none"]: return ""
      if re.fullmatch(r"\.\d+", v): return ""
      return v

  scan = pd.read_excel("ls.xlsx", header=None, dtype=str)
  test = pd.read_excel("TEST_ls.xlsx", header=None, dtype=str)

  scan.columns = range(scan.shape[1])
  test.columns = range(test.shape[1])

  scan[1] = scan[1].apply(clean_id)
  test[0] = test[0].apply(clean_id)

  lookup = {
      clean_id(row[0]): [clean(x) for x in row.tolist()]
      for _, row in test.iterrows()
      if clean_id(row[0])
  }

  column_map = {84:2,85:3,86:4,87:5,88:6,89:7,90:8,91:9,92:10}

  while scan.shape[1] < 334:
      scan[scan.shape[1]] = ""

  for i in range(len(scan)):
      key = scan.iat[i,1]
      if key not in lookup:
          continue
      test_row = lookup[key]
      for scan_col,test_col in column_map.items():
          if test_col < len(test_row):
              scan.iat[i,scan_col] = clean(test_row[test_col])

  scan.to_excel("Annonces.xlsx", header=False, index=False)

  with open("Annonces.csv","w",encoding="utf-8") as f:
      for _, row in scan.iterrows():
          f.write(DELIMITER.join(f'"{clean(x)}"' for x in row) + "\n")

  print("✅ Annonces.xlsx + Annonces.csv written")
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
  # STEP 6 — FTP UPLOAD
  # =========================================================

  from ftplib import FTP

  print("\n=== STEP 6: FTP UPLOAD ===")

  FTP_HOST = "ftp.figarocms.fr"
  FTP_USER = "tld-maisonvictoire"
  FTP_PASS = "Jvexn^bF%4"

  try:
      ftp = FTP(FTP_HOST, timeout=30)
      ftp.login(FTP_USER, FTP_PASS)

      print("Connected to FTP")

      with open(ZIP_NAME, "rb") as f:
          ftp.storbinary(f"STOR {ZIP_NAME}", f)

      ftp.quit()

      print(f"✅ Uploaded {ZIP_NAME} to FTP successfully")

  except Exception as e:
      print("❌ FTP upload failed:", e)

if __name__ == "__main__":
    run_pipeline()
