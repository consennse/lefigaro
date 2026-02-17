from fastapi import FastAPI
from pydantic import BaseModel
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import json
import zipfile
import re
import tempfile
import os
from ftplib import FTP

app = FastAPI()

DELIMITER = "!#"

class RunRequest(BaseModel):
    source_url: str
    ftp_host: str
    ftp_username: str
    ftp_password: str
    ftp_target_path: str


# =========================================================
# PIPELINE (YOUR SCRIPT)
# =========================================================

def run_pipeline(cfg: RunRequest):

    # temp working directory
    work = tempfile.mkdtemp()

    CSV_NAME = os.path.join(work, "scan.csv")
    TEST_CSV = os.path.join(work, "TEST.csv")
    XLS1 = os.path.join(work, "ls.xlsx")
    XLS2 = os.path.join(work, "TEST_ls.xlsx")
    OUT_XLSX = os.path.join(work, "Annonces.xlsx")
    OUT_CSV = os.path.join(work, "Annonces.csv")
    ZIP_NAME = os.path.join(work, "final.zip")

    # ---------------- XML DOWNLOAD ----------------

    r = requests.get(cfg.source_url, timeout=180)
    root = ET.fromstring(r.content)
    listings = root.findall(".//listing")

    # ---------------- SIMPLE CSV ----------------

    with open(CSV_NAME, "w", encoding="utf-8") as f:
        for listing in listings:
            row = [
                f'"{listing.findtext("id","")}"',
                f'"{listing.findtext("general_listing_information/listingprice","")}"'
            ]
            f.write(DELIMITER.join(row) + "\n")

    df = pd.read_csv(CSV_NAME, sep="!#", engine="python", header=None)
    df = df.map(lambda x: x.strip('"') if isinstance(x,str) else x)
    df.to_excel(XLS1, index=False, header=False)

    # ---------------- IMAGES ----------------

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
        row = [f'"{listing.findtext("id","")}"']
        for p in extract_images(listing):
            row.append(f'"{p}"')
        rows.append(row)

    with open(TEST_CSV,"w",encoding="utf-8") as f:
        for r in rows:
            f.write(DELIMITER.join(r) + "\n")

    df = pd.read_csv(TEST_CSV, sep="!#", engine="python", header=None)
    df = df.map(lambda x: x.strip('"') if isinstance(x,str) else x)
    df.to_excel(XLS2, index=False, header=False)

    # ---------------- MERGE ----------------

    scan = pd.read_excel(XLS1, header=None, dtype=str)
    test = pd.read_excel(XLS2, header=None, dtype=str)

    lookup = {
        str(row[0]): row.tolist()
        for _, row in test.iterrows()
        if str(row[0])
    }

    for i in range(len(scan)):
        key = str(scan.iat[i,0])
        if key not in lookup:
            continue

    scan.to_excel(OUT_XLSX, header=False, index=False)

    with open(OUT_CSV,"w",encoding="utf-8") as f:
        for _, row in scan.iterrows():
            f.write(DELIMITER.join(f'"{x}"' for x in row) + "\n")

    # ---------------- ZIP ----------------

    with zipfile.ZipFile(ZIP_NAME, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(OUT_CSV, arcname="Annonces.csv")

        z.writestr("config.txt",
            "Version=4.12\r\n"
            "Application=Propertybase / 3.0\r\n"
            "Devise=Euro\r\n"
        )

        z.writestr("photos.cfg", "Mode=URL\r\n")

    # ---------------- FTP ----------------

    ftp = FTP(cfg.ftp_host, timeout=60)
    ftp.login(cfg.ftp_username, cfg.ftp_password)

    with open(ZIP_NAME, "rb") as f:
        ftp.storbinary(f"STOR {cfg.ftp_target_path}", f)

    ftp.quit()

    # cleanup
    for file in os.listdir(work):
        os.remove(os.path.join(work,file))
    os.rmdir(work)

    return len(listings)


# =========================================================
# API ENDPOINT
# =========================================================

@app.post("/run")
def run_job(cfg: RunRequest):
    try:
        count = run_pipeline(cfg)
        return {"status": "success", "listings": count}
    except Exception as e:
        return {"status": "error", "message": str(e)}
