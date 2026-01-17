# 🌱 Overview
`fetch_bulk_JGI.py` is a JGI bulk genome downloader

# ⚙️ Installation
Clone this repository:
```bash
git clone https://github.com/AlicePierce/fetch_bulk_JGI.git
cd fetch_bulk_JGI
```

# 🚀 Quickstart
1. In bash:
```bash
export JGI_TOKEN="PASTE_TOKEN_HERE"
```
2. Create `queries.csv` using the following format:
```
Pvulgaris,PASTE_API_SEARCH_QUERY_LINK_HERE
Athaliana,PASTE_API_SEARCH_QUERY_LINK_HERE
```
3. run `fetch_bulk_jgi.py` to download genomess
```bash
# download GFF only from latest phytozome version
python fetch_bulk_jgi.py queries.csv \
  --include "*.gff3.gz" --include "*.gff.gz" \
  --latest-only \
  --outdir downloads
```

# 🧱 Dependencies
* Python 3.9+
* Valid [JGI session token](https://sites.google.com/lbl.gov/data-portal-help/home/tips_tutorials/api-tutorial#h.x3ip0t5de8fn)
* Network access to
    * `files.jgi.doe.gov`
    * `files-download.jgi.doe.gov`
    * Host API search URLs in CSV format (see below) 

# 🪙 JGI session token
1. Log into the JGI data portal in your browser
2. Open your user menu and copy your **session token** to your clipboard
3. On shell:
```bash
export JGI_TOKEN="PASTE_TOKEN_HERE"
```
For more info visit [here](https://sites.google.com/lbl.gov/data-portal-help/home/tips_tutorials/api-tutorial#h.x3ip0t5de8fn)
# 🔗 API search URLs
Create a CSV file (e.g. `queries.csv`) which will contain your API search queries necessary to download the desired datasets 

In the [JGI search portal](https://data.jgi.doe.gov/search?), locate the desired dataset and then copy the **API Search Query** to your clipboard and paste it in your CSV file. Example format:
```
Pvulgaris,PASTE_API_SEARCH_QUERY_LINK_HERE
Athaliana,PASTE_API_SEARCH_QUERY_LINK_HERE
```

# ⛳️ Flags
## Command-line options

| Option | Description | Example |
|------|-------------|---------|
| `--include GLOB` | Keep only files whose **file_name** matches the glob pattern. Can be repeated. | `--include "*.gff3.gz" --include "*.gff.gz"` |
| `--exclude GLOB` | Drop files whose **file_name** matches the glob pattern. Can be repeated. | `--exclude "*softmasked*"` |
| `--latest-only` | After include/exclude filtering, keep **only the newest detected PhytozomeV## version** per query. | `--latest-only` |
| `--outdir DIR` | Output directory for all downloads. | `--outdir downloads` |
| `--poll-seconds N` | Seconds between restore-status polls for PURGED files. | `--poll-seconds 600` |
| `--max-wait-seconds N` | Maximum time to wait for restore before aborting (in seconds). Default = 6 hours. | `--max-wait-seconds 21600` |
| `--keep-zip` | Keep the downloaded ZIP file instead of deleting it after unzip. | `--keep-zip` |

# 🏃🏻‍♀️ Run examples
## Download only GFF files from newest Phytozome version
```bash
python fetch_bulk_jgi.py queries.csv \
  --include "*.gff3.gz" --include "*.gff.gz" \
  --latest-only \
  --outdir downloads
```

## Download FASTA + GFF from newest Phytozome version
```bash
python fetch_bulk_jgi.py queries.csv \
  --include "*.fa.gz" --include "*.fasta.gz" --include "*.fna.gz" \
  --include "*.gff3.gz" --include "*.gff.gz" \
  --latest-only \
  --outdir downloads
```
## Download all files
```bash
python fetch_bulk_jgi.py queries.csv --outdir downloads
```

# 📃 Manifest file
`manifest.tsv` produced during the download process helps debug and include/exclude globs and version filtering

`manifest.tsv` contains the following columns:
* dataset_id
* file_id
* file_name
* file_status
* phytozome_version
* selected (true/false)

# 🔨 Troubleshooting
## Token/auth errors
Your JGI_TOKEN may have expired. Re-copy your session token from the portal and re-export it \
Explore the JGI Data Portal Help Website [here](https://sites.google.com/lbl.gov/data-portal-help/home)

# 📧 Getting help
This repository is maintained by [Alice](https://github.com/AlicePierce) \
For questions or collaborations, feel free to reach out by [email](avpierce@ucdavis.edu) \
Submit an issue [here](https://github.com/AlicePierce/fetch_bulk_JGI/issues)
