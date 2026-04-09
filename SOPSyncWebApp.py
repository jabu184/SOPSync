from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import sqlite3, os, pandas as pd, zipfile, shutil
import mimetypes
from io import BytesIO
from datetime import datetime

# Import the extraction function from your specific filename
from SOPSync_To_DB import run_extraction, init_db

app = Flask(__name__)
app.secret_key = "sop_dashboard_secure_key_2026"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'sop_data.db')

def get_db_connection():
    conn = init_db(BASE_DIR)
    conn.row_factory = sqlite3.Row
    return conn

def parse_sop_date(date_str):
    if not date_str: return datetime(9999, 12, 31).date()
    try:
        return datetime.strptime(date_str.strip(), "%d %b %Y").date()
    except:
        return datetime(9999, 12, 31).date()

@app.route('/sync-folders', methods=['GET', 'POST'])
def sync_folders():
    from tkinter import Tk, filedialog
    
    # Open a native folder selection dialog (shows up on the host machine)
    root = Tk()
    root.attributes('-topmost', True)
    root.withdraw()
    custom_path = filedialog.askdirectory(title="Select folder to search for SOP PDFs")
    root.destroy()
    
    if not custom_path:
        flash("Sync Cancelled: No folder was selected.", "warning")
        return redirect(url_for('index'))
    
    sync_file = os.path.join(BASE_DIR, 'last_sync.txt')
    if os.path.exists(sync_file):
        with open(sync_file, 'r') as f:
            last_synced_folder = f.read().strip()
            
        if last_synced_folder and custom_path != last_synced_folder:
            # Clear the database and Formatted folder to reset the collection
            conn = get_db_connection()
            conn.execute("DELETE FROM sops")
            conn.commit()
            conn.close()
            
            formatted_dir = os.path.join(BASE_DIR, 'Formatted')
            if os.path.exists(formatted_dir):
                shutil.rmtree(formatted_dir, ignore_errors=True)

    try:
        updated_count = run_extraction(search_path=custom_path)
        
        # Save the successfully synced folder to a local cache file
        with open(sync_file, 'w') as f:
            f.write(custom_path)
            
        flash(f"Sync Complete! {updated_count} records refreshed from: {custom_path}", "success")
    except Exception as e:
        flash(f"Sync Failed: {str(e)}", "danger")
    
    return redirect(url_for('index'))

@app.route('/sync-previous', methods=['GET', 'POST'])
def sync_previous():
    sync_file = os.path.join(BASE_DIR, 'last_sync.txt')
    if not os.path.exists(sync_file):
        flash("No previous sync folder recorded.", "warning")
        return redirect(url_for('index'))
        
    with open(sync_file, 'r') as f:
        custom_path = f.read().strip()
        
    if not custom_path or not os.path.exists(custom_path):
        flash("Previous sync folder not found or invalid.", "danger")
        return redirect(url_for('index'))
        
    try:
        updated_count = run_extraction(search_path=custom_path)
        flash(f"Sync Complete! {updated_count} records refreshed from: {custom_path}", "success")
    except Exception as e:
        flash(f"Sync Failed: {str(e)}", "danger")
        
    return redirect(url_for('index'))

@app.route('/open_pdf/<doc_id>')
def open_pdf(doc_id):
    """
    PRIORITY ROUTE: 
    1. Try the 'original_path' (The Physics Folder)
    2. Fallback to 'Formatted' folder (The Backup)
    """
    conn = get_db_connection()
    row = conn.execute("SELECT original_path FROM sops WHERE id = ?", (doc_id,)).fetchone()
    conn.close()
    
    if row and row['original_path']:
        target = row['original_path']
        # Check if the original file exists at the source
        if os.path.exists(target):
            mime_type, _ = mimetypes.guess_type(target)
            return send_file(target, mimetype=mime_type or 'application/octet-stream')
    
    # If original is missing, try to find the document in the renamed backup folder
    formatted_dir = os.path.join(BASE_DIR, 'Formatted')
    if os.path.exists(formatted_dir):
        for f in os.listdir(formatted_dir):
            if f.startswith(f"{doc_id}."):
                fallback = os.path.join(formatted_dir, f)
                mime_type, _ = mimetypes.guess_type(fallback)
                return send_file(fallback, mimetype=mime_type or 'application/octet-stream')
        
    return f"Error: Document {doc_id} not found at source or backup.", 404

@app.route('/export-csv')
def export_csv():
    conn = get_db_connection()
    search = request.args.get('search', '').strip()
    current_filter = request.args.get('filter', 'all')

    query = "SELECT filename, title, ref AS doc_ref, version, authors, approved AS approved_by, issue_date, next_review, keywords, id AS uniqueid, is_archived FROM sops WHERE is_archived = 0"
    params = []
    if search:
        query += " AND (title LIKE ? OR id LIKE ? OR authors LIKE ?)"
        params = [f'%{search}%', f'%{search}%', f'%{search}%']

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    today = datetime.now().date()
    def get_status(row):
        if row['is_archived'] == 1: return "Archived"
        expiry = parse_sop_date(row['next_review'])
        if expiry < today: return "Expired"
        if (expiry - today).days < 90: return "Due Soon"
        return "Current"

    df['Status'] = df.apply(get_status, axis=1)
    
    # Filter export based on dashboard state
    if current_filter == 'expired': df = df[df['Status'] == 'Expired']
    elif current_filter == 'warning': df = df[df['Status'] == 'Due Soon']
    elif current_filter == 'archived': df = df[df['is_archived'] == 1]
    elif current_filter == 'all': df = df[df['is_archived'] == 0]

    # Drop helper columns used for filtering so they don't appear in the CSV
    df = df.drop(columns=['Status', 'is_archived'])

    output = BytesIO()
    csv_data = df.to_csv(index=False)
    output.write(csv_data.encode('utf-8'))
    
    output.seek(0)
    return send_file(output, mimetype='text/csv', download_name=f"SOP_Export_{datetime.now().strftime('%Y%m%d')}.csv", as_attachment=True)

@app.route('/archive/<doc_id>')
def toggle_archive(doc_id):
    conn = get_db_connection()
    conn.execute("UPDATE sops SET is_archived = 1 - is_archived WHERE id = ?", (doc_id,))
    conn.commit()
    conn.close()
    return redirect(request.referrer or url_for('index'))

@app.route('/download-all')
def download_all():
    conn = get_db_connection()
    search = request.args.get('search', '').strip()
    current_filter = request.args.get('filter', 'all')

    # Mirror the search query from the main index route
    query = "SELECT id, next_review, is_archived FROM sops"
    params = []
    if search:
        query += " WHERE title LIKE ? OR keywords LIKE ? OR id LIKE ? OR authors LIKE ?"
        params = [f'%{search}%', f'%{search}%', f'%{search}%', f'%{search}%']

    rows = conn.execute(query, params).fetchall()
    conn.close()

    today = datetime.now().date()
    valid_ids = set()

    for row in rows:
        d = dict(row)
        expiry = parse_sop_date(d['next_review'])
        is_archived = d.get('is_archived') == 1
        expired = expiry < today
        days_left = (expiry - today).days

        if is_archived:
            status = 'archived'
        elif expired:
            status = 'expired'
        elif days_left < 90:
            status = 'warning'
        else:
            status = 'current'
            
        if current_filter == 'archived' and status == 'archived':
            valid_ids.add(d['id'])
        elif current_filter == 'expired' and status == 'expired':
            valid_ids.add(d['id'])
        elif current_filter == 'warning' and status == 'warning':
            valid_ids.add(d['id'])
        elif current_filter == 'all' and status != 'archived':
            valid_ids.add(d['id'])

    formatted_dir = os.path.join(BASE_DIR, 'Formatted')
    memory_file = BytesIO()
    
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(formatted_dir):
            for root, dirs, files in os.walk(formatted_dir):
                for file in files:
                    if file.lower().endswith(('.pdf', '.xlsx', '.xls', '.csv', '.xlsm')):
                        file_id = os.path.splitext(file)[0]
                        if file_id in valid_ids:
                            file_path = os.path.join(root, file)
                            # Add file to zip archive directly at the root level of the zip
                            zf.write(file_path, arcname=file)
                        
    memory_file.seek(0)
    return send_file(memory_file, 
                     mimetype='application/zip', 
                     download_name=f"SOP_Formatted_PDFs_{datetime.now().strftime('%Y%m%d')}.zip", 
                     as_attachment=True)

@app.route('/')
def index():
    conn = get_db_connection()
    search = request.args.get('search', '').strip()
    sort_by = request.args.get('sort', 'next_review')
    order = request.args.get('order', 'asc')
    current_filter = request.args.get('filter', 'all')

    query = "SELECT * FROM sops"
    params = []
    if search:
        query += " WHERE title LIKE ? OR keywords LIKE ? OR id LIKE ? OR authors LIKE ?"
        params = [f'%{search}%', f'%{search}%', f'%{search}%', f'%{search}%']

    rows = conn.execute(query, params).fetchall()
    conn.close()

    sops = []
    stats = {'total': 0, 'current': 0, 'warning': 0, 'expired': 0, 'archived': 0}
    today = datetime.now().date()

    for row in rows:
        d = dict(row)
        expiry = parse_sop_date(d['next_review'])
        d['sort_date'] = expiry
        d['days_left'] = (expiry - today).days
        d['expired'] = expiry < today
        
        ext = os.path.splitext(d.get('filename', ''))[1].lower()
        if ext == '.pdf':
            d['file_icon'] = '📄 PDF'
        elif ext in ['.xlsx', '.xls', '.csv', '.xlsm']:
            d['file_icon'] = '📊 Excel'
        else:
            d['file_icon'] = '📁 File'

        if d.get('is_archived') == 1:
            stats['archived'] += 1
        else:
            stats['total'] += 1
            if d['expired']: stats['expired'] += 1
            elif d['days_left'] < 90: stats['warning'] += 1
            else: stats['current'] += 1
        sops.append(d)

    reverse = (order == 'desc')
    if sort_by == 'next_review':
        sops.sort(key=lambda x: x['sort_date'], reverse=reverse)
    else:
        sops.sort(key=lambda x: str(x.get(sort_by, '')).lower(), reverse=reverse)

    sync_file = os.path.join(BASE_DIR, 'last_sync.txt')
    last_synced_folder = None
    if os.path.exists(sync_file):
        with open(sync_file, 'r') as f:
            last_synced_folder = f.read().strip()

    return render_template('index.html', sops=sops, search=search, sort=sort_by, order=order, stats=stats, current_filter=current_filter, last_synced_folder=last_synced_folder)

if __name__ == '__main__':
    # Change host to '0.0.0.0' to listen on all network interfaces
    app.run(host='0.0.0.0', port=5000)