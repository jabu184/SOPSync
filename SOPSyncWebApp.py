from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import sqlite3, os, pandas as pd, zipfile, shutil
import mimetypes
from io import BytesIO
from datetime import datetime
import threading
import time

# Import the extraction function from your specific filename
from SOPSync_To_DB import run_extraction, init_db, parse_file, load_author_mapping

app = Flask(__name__)
app.secret_key = "sop_dashboard_secure_key_2026"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'sop_data.db')

scan_lock = threading.Lock()
last_auto_scan_time = 0

def background_scan_task(folder):
    try:
        run_extraction(search_path=folder)
    finally:
        scan_lock.release()

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
    if request.method == 'GET':
        return redirect(url_for('index'))
        
    if request.form.get('password') != 'admin':
        flash("Unauthorized: Incorrect password.", "danger")
        return redirect(url_for('index'))
        
    if 'folder_files' not in request.files:
        flash("Sync Cancelled: No folder was uploaded.", "warning")
        return redirect(url_for('index'))
        
    files = request.files.getlist('folder_files')
    
    if not files or files[0].filename == '':
        flash("Sync Cancelled: No files were found.", "warning")
        return redirect(url_for('index'))
        
    try:
        with scan_lock:
            upload_dir = os.path.join(BASE_DIR, 'Uploaded_SOPs')
            
            sync_file = os.path.join(BASE_DIR, 'last_sync.txt')
            if os.path.exists(sync_file):
                with open(sync_file, 'r') as f:
                    last_synced_folder = f.read().strip()
                    
                if last_synced_folder:
                    # Clear the database and Formatted folder to reset the collection for the new upload
                    conn = get_db_connection()
                    conn.execute("DELETE FROM sops")
                    conn.commit()
                    conn.close()
                    
                    formatted_dir = os.path.join(BASE_DIR, 'Formatted')
                    if os.path.exists(formatted_dir):
                        shutil.rmtree(formatted_dir, ignore_errors=True)

            if os.path.exists(upload_dir):
                shutil.rmtree(upload_dir, ignore_errors=True)
            os.makedirs(upload_dir, exist_ok=True)

            for file in files:
                if file.filename:
                    # Preserve directory structure and sanitize path
                    clean_name = file.filename.lstrip('/\\')
                    safe_path = os.path.abspath(os.path.join(upload_dir, clean_name))
                    
                    # Prevent directory traversal to keep the server secure
                    if safe_path.startswith(os.path.abspath(upload_dir)):
                        os.makedirs(os.path.dirname(safe_path), exist_ok=True)
                        file.save(safe_path)
                        
            updated_count = run_extraction(search_path=upload_dir)
            
            # Save the successfully synced folder to a local cache file
            with open(sync_file, 'w') as f:
                f.write(upload_dir)
            
        flash(f"Sync Complete! {updated_count} records refreshed from the uploaded folder.", "success")
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
        with scan_lock:
            updated_count = run_extraction(search_path=custom_path)
        flash(f"Sync Complete! {updated_count} records refreshed from: {custom_path}", "success")
    except Exception as e:
        flash(f"Sync Failed: {str(e)}", "danger")
        
    return redirect(url_for('index'))

@app.route('/upload-single', methods=['POST'])
def upload_single():
    if request.form.get('password') != 'admin':
        flash("Unauthorized: Incorrect password.", "danger")
        return redirect(url_for('index'))

    if 'single_file' not in request.files:
        flash("No file uploaded.", "warning")
        return redirect(url_for('index'))
    
    file = request.files['single_file']
    if file.filename == '':
        flash("No file selected.", "warning")
        return redirect(url_for('index'))
        
    staging_dir = os.path.join(BASE_DIR, 'staging')
    os.makedirs(staging_dir, exist_ok=True)
    
    clean_name = file.filename.lstrip('/\\')
    safe_path = os.path.abspath(os.path.join(staging_dir, clean_name))
    file.save(safe_path)
    
    # Isolate extraction before writing anything to the main library directory
    author_map = load_author_mapping(BASE_DIR)
    parsed = parse_file(safe_path, author_map)
    
    if not parsed:
        flash("Failed to extract metadata from the uploaded file.", "danger")
        os.remove(safe_path)
        return redirect(url_for('index'))
        
    conn = get_db_connection()
    existing = conn.execute("SELECT * FROM sops WHERE id = ?", (parsed['id'],)).fetchone()
    conn.close()
    
    if existing:
        # Prompt the user with differences before finalizing
        return render_template('confirm_override.html', new_doc=parsed, existing_doc=dict(existing), tmp_file=clean_name)
    
    return process_single_upload(clean_name)

@app.route('/confirm-override', methods=['POST'])
def confirm_override():
    tmp_file = request.form.get('tmp_file')
    if request.form.get('action') == 'cancel':
        staging_file = os.path.join(BASE_DIR, 'staging', tmp_file)
        if os.path.exists(staging_file): os.remove(staging_file)
        flash("Upload cancelled.", "info")
        return redirect(url_for('index'))
        
    return process_single_upload(tmp_file)

def process_single_upload(tmp_file):
    with scan_lock:
        staging_file = os.path.join(BASE_DIR, 'staging', tmp_file)
        
        # 1. Determine the currently active sync folder to avoid splitting the library
        sync_file = os.path.join(BASE_DIR, 'last_sync.txt')
        target_dir = os.path.join(BASE_DIR, 'Uploaded_SOPs')
        if os.path.exists(sync_file):
            with open(sync_file, 'r') as f:
                saved_dir = f.read().strip()
                if saved_dir and os.path.exists(saved_dir):
                    target_dir = saved_dir
                    
        os.makedirs(target_dir, exist_ok=True)
        
        # 2. Extract ID to prevent duplicate files and force a clean DB update
        author_map = load_author_mapping(BASE_DIR)
        parsed = parse_file(staging_file, author_map)
        
        if parsed:
            conn = get_db_connection()
            existing = conn.execute("SELECT original_path FROM sops WHERE id = ?", (parsed['id'],)).fetchone()
            
            if existing and existing['original_path'] and os.path.exists(existing['original_path']):
                # If the file being overwritten has a different filename, delete the old file
                old_path = os.path.abspath(existing['original_path'])
                new_path = os.path.abspath(os.path.join(target_dir, tmp_file))
                if old_path != new_path:
                    try: os.remove(old_path)
                    except: pass
                    
            # Delete the existing DB record to force the scanner to parse the new document completely
            conn.execute("DELETE FROM sops WHERE id = ?", (parsed['id'],))
            conn.commit()
            conn.close()
            
            # Clear out any old versions in the Formatted cache
            formatted_dir = os.path.join(BASE_DIR, 'Formatted')
            if os.path.exists(formatted_dir):
                for f in os.listdir(formatted_dir):
                    if f.startswith(f"{parsed['id']}."):
                        try: os.remove(os.path.join(formatted_dir, f))
                        except: pass
        
        dest_path = os.path.join(target_dir, tmp_file)
        
        # 3. Safely move the file (preventing Windows FileExistsError)
        if os.path.exists(dest_path):
            try: os.remove(dest_path)
            except: pass
            
        shutil.move(staging_file, dest_path)
        
        # 4. Re-scan the folder to pick up the fresh document
        updated = run_extraction(search_path=target_dir)
        
        with open(sync_file, 'w') as f: 
            f.write(target_dir)
        
    flash(f"File uploaded successfully! Records updated: {updated}", "success")
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
    global last_auto_scan_time
    # Trigger an automatic ultra-fast scan on every load
    sync_file = os.path.join(BASE_DIR, 'last_sync.txt')
    last_synced_folder = None
    if os.path.exists(sync_file):
        with open(sync_file, 'r') as f:
            last_synced_folder = f.read().strip()
            
    if last_synced_folder and os.path.exists(last_synced_folder):
        current_time = time.time()
        # Only scan if it's been more than 5 seconds since the last scan to prevent spam
        if current_time - last_auto_scan_time > 5:
            if scan_lock.acquire(blocking=False):
                last_auto_scan_time = current_time
                # Run in background so the UI doesn't freeze during the scan
                threading.Thread(target=background_scan_task, args=(last_synced_folder,)).start()
        
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

    return render_template('index.html', sops=sops, search=search, sort=sort_by, order=order, stats=stats, current_filter=current_filter, last_synced_folder=last_synced_folder)

if __name__ == '__main__':
    # Change host to '0.0.0.0' to listen on all network interfaces
    app.run(host='0.0.0.0', port=5000)