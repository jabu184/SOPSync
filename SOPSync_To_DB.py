import pdfplumber
import os
import sqlite3
import re
import shutil
import pandas as pd
from datetime import datetime
import sys

# --- 1. HELPER FUNCTIONS (Must be defined before run_extraction) ---

def load_author_mapping(root_dir):
    mapping = {}
    auth_file = os.path.join(root_dir, 'authors.txt')
    
    # Fallback to parent directory if running inside a department subfolder
    if not os.path.exists(auth_file) and 'departments' in os.path.abspath(root_dir):
        auth_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(root_dir))), 'authors.txt')
        
    if os.path.exists(auth_file):
        try:
            with open(auth_file, 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        mapping[parts[0].strip()] = parts[1].strip()
        except Exception as e:
            print(f"Warning: Could not read authors.txt: {e}")
    return mapping

def replace_initials(text, mapping):
    if not text: return ""
    comma_val = text.replace('/', ', ').replace('\\', ', ')
    parts = [p.strip() for p in comma_val.split(',')]
    resolved = [mapping.get(p, p) for p in parts]
    return ", ".join(resolved)

def init_db(root_dir):
    db_path = os.path.join(root_dir, 'sop_data.db')
    conn = sqlite3.connect(db_path, timeout=20.0)
    conn.execute('''CREATE TABLE IF NOT EXISTS sops 
        (id TEXT PRIMARY KEY, title TEXT, ref TEXT, version TEXT, authors TEXT, 
         approved TEXT, issue_date TEXT, next_review TEXT, keywords TEXT, 
         filename TEXT, last_updated TEXT, is_archived INTEGER DEFAULT 0,
         original_path TEXT, file_mtime REAL DEFAULT 0)''')
         
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(sops)")
        cols = [c[1] for c in cursor.fetchall()]
        if 'file_mtime' not in cols:
            conn.execute("ALTER TABLE sops ADD COLUMN file_mtime REAL DEFAULT 0")
    except:
        pass
    return conn

def parse_file(full_path, author_map):
    filename = os.path.basename(full_path)
    if filename.lower().endswith('.pdf'):
        try:
            with pdfplumber.open(full_path) as pdf:
                page = pdf.pages[0]
                tables = page.extract_tables()
                if not tables: return None
                
                pdf_data = {}
                for row in tables[0]:
                    if len(row) >= 2:
                        lbl = str(row[0]).lower()
                        val = str(row[1]).strip()
                        if 'reference' in lbl: pdf_data['ref'] = val
                        elif 'version' in lbl: pdf_data['ver'] = val
                        elif 'next review' in lbl: pdf_data['next'] = val
                        elif 'issue date' in lbl: pdf_data['issue'] = val

                doc_ref = pdf_data.get('ref', 'XX-0')
                prefix = doc_ref[:2].upper()
                ref_num = "".join(re.findall(r'\d+', doc_ref))
                uid = f"{prefix}{ref_num}"

                in_keywords = False
                keywords_list = []
                for row in tables[0]:
                    lbl = str(row[0]).lower() if row[0] else ""
                    if lbl.strip() and 'keyword' not in lbl:
                        in_keywords = False
                    if 'keyword' in lbl:
                        in_keywords = True
                    if in_keywords:
                        for cell in row[1:]:
                            if cell:
                                for word in str(cell).split('\n'):
                                    if word.strip() and word.strip().lower() != 'none':
                                        keywords_list.append(word.strip())
                    elif len(row) >= 2:
                        val = str(row[1]).strip() if row[1] else ""
                        if 'author' in lbl: pdf_data['auth'] = val
                        elif 'approved' in lbl: pdf_data['appr'] = val
                        elif 'issue date' in lbl and 'issue' not in pdf_data: pdf_data['issue'] = val
                
                full_text = page.extract_text()
                title_match = re.search(r"(?:Trust|Centre|NHS|Hertfordshire)\s*\n(.*?)\nDocument reference", full_text, re.DOTALL)
                title = title_match.group(1).replace('\n', ' ').strip() if title_match else "Unknown Title"
                
                return {
                    'id': uid, 'title': title, 'ref': doc_ref, 'ver': pdf_data.get('ver', ''),
                    'auth': replace_initials(pdf_data.get('auth', ''), author_map),
                    'appr': replace_initials(pdf_data.get('appr', ''), author_map),
                    'issue': pdf_data.get('issue', ''), 'next': pdf_data.get('next', ''),
                    'keywords': ", ".join(keywords_list), 'filename': filename
                }
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            return None
            
    elif filename.lower().endswith(('.xlsx', '.xls', '.csv', '.xlsm')):
        try:
            if filename.lower().endswith('.csv'): df = pd.read_csv(full_path, header=None, nrows=50)
            else: df = pd.read_excel(full_path, header=None, engine='openpyxl' if filename.lower().endswith(('.xlsx', '.xlsm')) else None)
            
            if df.empty or len(df.columns) < 2: return None
                
            def get_cell_val(r, c, is_date=False):
                if r < len(df) and c < len(df.columns):
                    val = df.iat[r, c]
                    if pd.isna(val): return ""
                    if is_date and isinstance(val, (datetime, pd.Timestamp)): return val.strftime("%d %b %Y")
                    return str(val).strip()
                return ""

            xls_data = {
                'title': get_cell_val(4, 1), 'ref': get_cell_val(8, 3), 'ver': get_cell_val(9, 3),
                'auth': get_cell_val(10, 3), 'appr': get_cell_val(11, 3), 'issue': get_cell_val(12, 3, is_date=True),
                'next': get_cell_val(13, 3, is_date=True)
            }
            
            raw_keywords = [get_cell_val(16, 3), get_cell_val(17, 3), get_cell_val(18, 3), get_cell_val(16, 6), get_cell_val(17, 6), get_cell_val(18, 6)]
            keywords_list = [w.strip() for cell in raw_keywords if cell for w in str(cell).split('\n') if w.strip() and w.strip().lower() != 'none']
            
            doc_ref = xls_data.get('ref', 'XX-0')
            uid = f"X{doc_ref[:2].upper()}{''.join(re.findall(r'\d+', doc_ref))}"
            
            return {
                'id': uid, 'title': xls_data.get('title') or os.path.splitext(filename)[0],
                'ref': doc_ref, 'ver': xls_data.get('ver', ''), 'auth': replace_initials(xls_data.get('auth', ''), author_map),
                'appr': replace_initials(xls_data.get('appr', ''), author_map), 'issue': xls_data.get('issue', ''),
                'next': xls_data.get('next', ''), 'keywords': ", ".join(keywords_list), 'filename': filename
            }
        except Exception as e:
            print(f"Error processing spreadsheet {filename}: {e}")
            return None
    return None

# --- 2. MAIN EXTRACTION ENGINE ---

def run_extraction(search_path=None, workspace_dir=None):
    if not workspace_dir:
        if getattr(sys, 'frozen', False):
            workspace_dir = os.path.dirname(sys.executable)
        else:
            workspace_dir = os.path.dirname(os.path.abspath(__file__))
        
    author_map = load_author_mapping(workspace_dir)
    conn = init_db(workspace_dir)
    cursor = conn.cursor()
    
    formatted_folder = os.path.join(workspace_dir, 'Formatted')
    if not os.path.exists(formatted_folder):
        os.makedirs(formatted_folder)

    files_updated = 0
    current_time = datetime.now().strftime("%d %b %Y %H:%M")
    target_dir = search_path if search_path else workspace_dir

    # BULK CACHE MTIMES TO PREVENT N+1 QUERIES
    cursor.execute("SELECT original_path, file_mtime FROM sops")
    mtime_cache = {row[0]: row[1] for row in cursor.fetchall()}

    for root, dirs, files in os.walk(target_dir):
        if "Formatted" in root: continue
        is_archived_file = 1 if 'archived' in root.lower() else 0

        for filename in files:
            if filename.lower().endswith(('.pdf', '.xlsx', '.xls', '.csv', '.xlsm')):
                full_path = os.path.abspath(os.path.join(root, filename))
                try:
                    mtime = os.path.getmtime(full_path)
                    
                    # FAST GATE: Check dictionary cache instead of querying the DB
                    if mtime_cache.get(full_path) == mtime:
                        continue
                        
                    parsed = parse_file(full_path, author_map)
                    if not parsed: continue
                    
                    uid = parsed['id']
                    
                    # SMART GATE: Secondary check for identical files that were moved/renamed locally
                    cursor.execute("SELECT version, next_review, original_path, file_mtime FROM sops WHERE id = ?", (uid,))
                    existing = cursor.fetchone()

                    if existing:
                        if str(existing[0]) == str(parsed['ver']) and \
                           str(existing[1]) == str(parsed['next']) and \
                           str(existing[2]) == str(full_path):
                            # Refresh the mtime just in case they touched the file without modifying it
                            cursor.execute("UPDATE sops SET file_mtime = ? WHERE id = ?", (mtime, uid))
                            if not is_archived_file:
                                try: shutil.copy2(full_path, os.path.join(formatted_folder, f"{uid}{os.path.splitext(filename)[1].lower()}"))
                                except: pass
                            continue 
                            
                        # Prevent old backup files in the folder from overwriting newly uploaded data
                        if mtime < existing[3]:
                            continue

                    cursor.execute("INSERT OR IGNORE INTO sops (id, is_archived) VALUES (?, ?)", (uid, is_archived_file))
                    
                    cursor.execute("""
                        UPDATE sops SET 
                            title=?, ref=?, version=?, authors=?, approved=?, 
                            issue_date=?, next_review=?, keywords=?, filename=?, 
                            last_updated=?, original_path=?, is_archived=?, file_mtime=?
                        WHERE id=?
                    """, (parsed['title'], parsed['ref'], parsed['ver'], parsed['auth'], parsed['appr'], 
                          parsed['issue'], parsed['next'], parsed['keywords'], parsed['filename'], 
                          current_time, full_path, is_archived_file, mtime, uid))
                    
                    if not is_archived_file:
                        try: shutil.copy2(full_path, os.path.join(formatted_folder, f"{uid}{os.path.splitext(filename)[1].lower()}"))
                        except: pass 

                    files_updated += 1
                    print(f"Updated: {uid} | Status Preserved | Source: {filename}")

                except Exception as e:
                    print(f"Error processing {filename}: {e}")

    conn.commit()
    conn.close()
    return files_updated

if __name__ == "__main__":
    import tkinter as tk
    from tkinter import filedialog
    
    # Allow standalone runs to also prompt for a folder
    root = tk.Tk()
    root.attributes('-topmost', True)
    root.withdraw()
    folder_selected = filedialog.askdirectory(title="Select folder to search for SOPs")
    root.destroy()
    
    run_extraction(search_path=folder_selected if folder_selected else None)