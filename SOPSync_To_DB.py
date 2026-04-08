import pdfplumber
import os
import sqlite3
import re
import shutil
from datetime import datetime

# --- 1. HELPER FUNCTIONS (Must be defined before run_extraction) ---

def load_author_mapping(root_dir):
    """Loads initials and names from authors.txt into a dictionary."""
    mapping = {}
    auth_file = os.path.join(root_dir, 'authors.txt')
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
    """Replaces initials with full names based on mapping."""
    if not text: return ""
    comma_val = text.replace('/', ', ').replace('\\', ', ')
    parts = [p.strip() for p in comma_val.split(',')]
    resolved = [mapping.get(p, p) for p in parts]
    return ", ".join(resolved)

def init_db(root_dir):
    """Initializes the database with all 13 required columns."""
    db_path = os.path.join(root_dir, 'sop_data.db')
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE IF NOT EXISTS sops 
        (id TEXT PRIMARY KEY, title TEXT, ref TEXT, version TEXT, authors TEXT, 
         approved TEXT, issue_date TEXT, next_review TEXT, keywords TEXT, 
         filename TEXT, last_updated TEXT, is_archived INTEGER DEFAULT 0,
         original_path TEXT)''')
    return conn

# --- 2. MAIN EXTRACTION ENGINE ---

def run_extraction(search_path=None):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    author_map = load_author_mapping(root_dir)
    conn = init_db(root_dir)
    cursor = conn.cursor()
    
    formatted_folder = os.path.join(root_dir, 'Formatted')
    if not os.path.exists(formatted_folder):
        os.makedirs(formatted_folder)

    files_updated = 0
    current_time = datetime.now().strftime("%d %b %Y %H:%M")

    # Use the provided search path, or fallback to the script directory
    target_dir = search_path if search_path else root_dir

    for root, dirs, files in os.walk(target_dir):
        # Skip the Formatted folder to prevent infinite loops
        if "Formatted" in root: continue

        for filename in files:
            if filename.lower().endswith('.pdf'):
                full_path = os.path.abspath(os.path.join(root, filename))
                
                try:
                    with pdfplumber.open(full_path) as pdf:
                        page = pdf.pages[0]
                        tables = page.extract_tables()
                        if not tables: continue
                        
                        pdf_data = {}
                        for row in tables[0]:
                            if len(row) >= 2:
                                lbl = str(row[0]).lower()
                                val = str(row[1]).strip()
                                if 'reference' in lbl: pdf_data['ref'] = val
                                elif 'version' in lbl: pdf_data['ver'] = val
                                elif 'next review' in lbl: pdf_data['next'] = val

                        # Generate Clean ID
                        doc_ref = pdf_data.get('ref', 'XX-0')
                        prefix = doc_ref[:2].upper()
                        ref_num = "".join(re.findall(r'\d+', doc_ref))
                        uid = f"{prefix}{ref_num}"

                        # --- SMART GATE: Check if changes exist ---
                        cursor.execute("SELECT version, next_review, original_path FROM sops WHERE id = ?", (uid,))
                        existing = cursor.fetchone()

                        if existing and str(existing[0]) == str(pdf_data.get('ver')) and \
                           str(existing[1]) == str(pdf_data.get('next')) and \
                           str(existing[2]) == str(full_path):
                            continue 

                        # Extraction for metadata
                        in_keywords = False
                        keywords_list = []
                        for row in tables[0]:
                            lbl = str(row[0]).lower() if row[0] else ""
                            
                            # If we hit a new label that isn't empty and isn't 'keyword', we leave keyword mode
                            if lbl.strip() and 'keyword' not in lbl:
                                in_keywords = False
                                
                            if 'keyword' in lbl:
                                in_keywords = True
                                
                            if in_keywords:
                                # Loop through all columns to the right of the label (handles 2+ columns)
                                for cell in row[1:]:
                                    if cell:
                                        # Split by newline to catch multiple keywords merged in a single cell
                                        for word in str(cell).split('\n'):
                                            if word.strip() and word.strip().lower() != 'none':
                                                keywords_list.append(word.strip())
                            elif len(row) >= 2:
                                val = str(row[1]).strip() if row[1] else ""
                                if 'author' in lbl: pdf_data['auth'] = val
                                elif 'approved' in lbl: pdf_data['appr'] = val
                                elif 'issue date' in lbl: pdf_data['issue'] = val
                        
                        pdf_data['keywords'] = ", ".join(keywords_list)

                        full_text = page.extract_text()
                        title_match = re.search(r"(?:Trust|Centre|NHS|Hertfordshire)\s*\n(.*?)\nDocument reference", full_text, re.DOTALL)
                        title = title_match.group(1).replace('\n', ' ').strip() if title_match else "Unknown Title"
                        
                        auths = replace_initials(pdf_data.get('auth', ''), author_map)
                        apprs = replace_initials(pdf_data.get('appr', ''), author_map)

                        # --- THE ARCHIVE FIX ---
                        # 1. Insert the record if it doesn't exist (default to active/0)
                        cursor.execute("INSERT OR IGNORE INTO sops (id, is_archived) VALUES (?, 0)", (uid,))
                        
                        # 2. Update all metadata but DO NOT TOUCH the is_archived column
                        cursor.execute("""
                            UPDATE sops SET 
                                title=?, ref=?, version=?, authors=?, approved=?, 
                                issue_date=?, next_review=?, keywords=?, filename=?, 
                                last_updated=?, original_path=?
                            WHERE id=?
                        """, (title, doc_ref, pdf_data.get('ver'), auths, apprs, 
                              pdf_data.get('issue', ''), pdf_data.get('next'), pdf_data.get('keywords', ''), filename, 
                              current_time, full_path, uid))
                        
                        # Update the renamed backup
                        try:
                            shutil.copy2(full_path, os.path.join(formatted_folder, f"{uid}.pdf"))
                        except:
                            pass 

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