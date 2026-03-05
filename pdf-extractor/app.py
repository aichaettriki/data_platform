from flask import Flask, request, send_file, jsonify
import subprocess, os, json, tempfile, threading, uuid
import pandas as pd

app = Flask(__name__)
jobs = {}

HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>MinerU - ITCEQ</title>
<style>
body{font-family:Arial,sans-serif;max-width:860px;margin:40px auto;padding:20px;background:#f4f1eb}
h1{color:#0d2137;font-size:28px;margin-bottom:6px}
p.sub{color:#888;margin-bottom:24px;font-size:14px}
.box{background:white;padding:24px;border-radius:10px;border:1px solid #ddd;margin-bottom:20px}
label{font-weight:600;font-size:14px;display:block;margin-bottom:8px;color:#0d2137}
input[type=file]{display:block;width:100%;padding:10px;border:2px dashed #ddd;border-radius:6px;background:#f9f9f9;font-size:14px;cursor:pointer;margin-bottom:12px}
input[type=file]:hover{border-color:#c8a84b}
.info{padding:10px 14px;border-radius:6px;font-size:13px;margin-bottom:12px;display:none}
.info-ok{background:#edfaed;border:1px solid #27ae60;color:#1a7a1a}
.info-warn{background:#fff8e1;border:1px solid #ffc107;color:#7a5c00}
.btn{padding:11px 24px;font-size:14px;font-weight:600;cursor:pointer;border:none;border-radius:6px;margin-right:8px}
#btnGo{background:#0d2137;color:white}
#btnGo:disabled{opacity:0.35;cursor:not-allowed}
#btnStop{background:white;color:#c0392b;border:1px solid #c0392b;display:none}
#btnDl{background:#c8a84b;color:#0d2137;display:none}
#statusMsg{margin-top:14px;font-size:14px;min-height:20px}
.prog{margin-top:12px;display:none}
.bar-bg{height:6px;background:#e0e0e0;border-radius:3px;overflow:hidden}
.bar-fg{height:100%;background:linear-gradient(90deg,#0d2137,#c8a84b);border-radius:3px;width:0%;transition:width 0.4s}
.steps{margin-top:12px;display:flex;flex-direction:column;gap:6px}
.step{font-size:12px;color:#aaa;display:flex;align-items:center;gap:8px}
.step.on{color:#0d2137;font-weight:600}
.step.done{color:#27ae60}
.dot{width:7px;height:7px;border-radius:50%;background:#ddd;flex-shrink:0}
.step.on .dot{background:#c8a84b}
.step.done .dot{background:#27ae60}
.res-title{font-size:22px;font-weight:700;color:#0d2137;margin-bottom:16px}
.tc{background:white;border:1px solid #ddd;border-radius:8px;margin-bottom:16px;overflow:hidden}
.tc-head{background:#0d2137;color:white;padding:12px 16px;display:flex;align-items:center;gap:10px}
.pg{background:#c8a84b;color:#0d2137;font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px}
.tc-body{overflow-x:auto;padding:16px}
table{border-collapse:collapse;width:100%;font-size:13px}
th{background:#f4f1eb;padding:9px 12px;text-align:left;border-bottom:2px solid #ddd;color:#0d2137}
td{padding:8px 12px;border-bottom:1px solid #eee}
tr:hover td{background:#fafafa}
</style>
</head>
<body>
<h1>MinerU &mdash; ITCEQ</h1>
<p class="sub">Extraction automatique de tableaux depuis vos documents PDF</p>

<div class="box">
  <label>Selectionner un fichier PDF :</label>
  <input type="file" id="f" accept=".pdf">
  <div id="dInfo" class="info info-ok"></div>
  <div id="dWarn" class="info info-warn"></div>
  <div>
    <button class="btn" id="btnGo" disabled>Lancer l'extraction</button>
    <button class="btn" id="btnStop">Arreter</button>
    <button class="btn" id="btnDl">Telecharger Excel</button>
  </div>
  <div id="statusMsg"></div>
  <div class="prog" id="prog">
    <div class="bar-bg"><div class="bar-fg" id="barFg"></div></div>
    <div class="steps">
      <div class="step" id="st1"><div class="dot"></div>Lecture du document</div>
      <div class="step" id="st2"><div class="dot"></div>Analyse de la mise en page</div>
      <div class="step" id="st3"><div class="dot"></div>Extraction des tableaux</div>
      <div class="step" id="st4"><div class="dot"></div>Generation du fichier Excel</div>
    </div>
  </div>
</div>

<div id="resBox"></div>

<script>
var file = null;
var jobId = null;
var timer = null;

document.getElementById("f").addEventListener("change", function() {
  var f = this.files[0];
  if (!f) return;
  file = f;
  var mb = (f.size / 1048576).toFixed(1);
  var di = document.getElementById("dInfo");
  di.textContent = "Fichier selectionne : " + f.name + " (" + mb + " MB)";
  di.style.display = "block";
  var dw = document.getElementById("dWarn");
  if (f.size > 30 * 1048576) {
    dw.textContent = "Fichier volumineux (" + mb + " MB) - l'analyse peut prendre plusieurs minutes.";
    dw.style.display = "block";
  } else {
    dw.style.display = "none";
  }
  document.getElementById("btnGo").disabled = false;
  document.getElementById("resBox").innerHTML = "";
  document.getElementById("btnDl").style.display = "none";
  document.getElementById("statusMsg").textContent = "";
  document.getElementById("prog").style.display = "none";
});

document.getElementById("btnGo").addEventListener("click", function() {
  if (!file) return;
  document.getElementById("btnGo").disabled = true;
  document.getElementById("btnStop").style.display = "inline-block";
  document.getElementById("btnDl").style.display = "none";
  document.getElementById("prog").style.display = "block";
  document.getElementById("resBox").innerHTML = "";
  setStatus("Envoi du fichier...", 5, 1);
  var fd = new FormData();
  fd.append("pdf", file);
  fetch("/start", {method:"POST", body:fd})
    .then(function(r){ return r.json(); })
    .then(function(d){
      if (d.error) { setStatus("Erreur: " + d.error, 0, 0); resetUI(); return; }
      jobId = d.job_id;
      setStatus("Analyse en cours...", 15, 2);
      timer = setInterval(poll, 2000);
    })
    .catch(function(e){ setStatus("Erreur reseau: " + e, 0, 0); resetUI(); });
});

document.getElementById("btnStop").addEventListener("click", function() {
  clearInterval(timer);
  if (jobId) fetch("/cancel/" + jobId, {method:"POST"});
  setStatus("Analyse arretee.", 0, 0);
  resetUI();
});

document.getElementById("btnDl").addEventListener("click", function() {
  if (jobId) window.location.href = "/download/" + jobId;
});

function poll() {
  fetch("/status/" + jobId)
    .then(function(r){ return r.json(); })
    .then(function(d){
      if (d.status === "running") {
        setStatus(d.message + " - " + d.progress + "%", d.progress, d.step);
      } else if (d.status === "done") {
        clearInterval(timer);
        setStatus("Termine ! " + d.count + " tableau(x) extrait(s).", 100, 5);
        showResults(d.tables, d.count);
        document.getElementById("btnDl").style.display = "inline-block";
        resetUI();
      } else if (d.status === "cancelled") {
        clearInterval(timer);
        setStatus("Analyse arretee.", 0, 0);
        resetUI();
      } else if (d.status === "error") {
        clearInterval(timer);
        setStatus("Erreur: " + (d.error || "inconnue"), 0, 0);
        resetUI();
      }
    });
}

function setStatus(msg, pct, step) {
  document.getElementById("statusMsg").textContent = msg;
  document.getElementById("barFg").style.width = pct + "%";
  for (var i = 1; i <= 4; i++) {
    var el = document.getElementById("st" + i);
    el.className = "step";
    if (i < step) el.classList.add("done");
    else if (i === step) el.classList.add("on");
  }
}

function resetUI() {
  document.getElementById("btnGo").disabled = false;
  document.getElementById("btnStop").style.display = "none";
}

function showResults(tables, count) {
  var c = document.getElementById("resBox");
  c.innerHTML = "<p class='res-title'>" + count + " tableau(x) trouve(s)</p>";
  for (var i = 0; i < tables.length; i++) {
    var t = tables[i];
    c.innerHTML += "<div class='tc'>"
      + "<div class='tc-head'><span class='pg'>Page " + t.page + "</span><span>" + t.title + "</span></div>"
      + "<div class='tc-body'>" + t.html + "</div>"
      + "</div>";
  }
}
</script>
</body>
</html>"""


def run_extraction(job_id, pdf_path, out_dir):
    job = jobs[job_id]
    try:
        job.update({"progress": 20, "message": "Analyse de la mise en page", "step": 2})
        proc = subprocess.Popen(
            ["mineru", "-p", pdf_path, "-o", out_dir, "-b", "pipeline"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        job["process"] = proc
        stdout, stderr = proc.communicate()
        if job["status"] == "cancelled":
            return
        job.update({"progress": 70, "message": "Extraction des tableaux", "step": 3})
        if proc.returncode != 0:
            job.update({"status": "error", "error": stderr.decode()[:500]})
            return
        json_files = []
        for root, _, files in os.walk(out_dir):
            for f in files:
                if f.endswith("_content_list.json"):
                    json_files.append(os.path.join(root, f))
        if not json_files:
            job.update({"status": "error", "error": "Aucun resultat genere."})
            return
        with open(json_files[0]) as f:
            content = json.load(f)
        tables = []
        for block in content:
            if block.get("type") == "table":
                page = (block.get("page_idx", 0) or 0) + 1
                caption = block.get("table_caption", [])
                title = " ".join(caption) if caption else "Tableau page " + str(page)
                html_table = block.get("table_body", "")
                try:
                    df = pd.read_html(html_table)[0] if html_table else pd.DataFrame()
                except:
                    df = pd.DataFrame({"Contenu": [html_table]})
                tables.append({"page": page, "title": title,
                               "html": df.to_html(index=False, border=0), "df": df})
        job.update({"progress": 85, "step": 4, "message": "Generation Excel"})
        excel_tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        with pd.ExcelWriter(excel_tmp.name, engine="openpyxl") as writer:
            for i, t in enumerate(tables):
                sheet = ("P" + str(t["page"]) + "_T" + str(i+1))[:31]
                pd.DataFrame([["Titre : " + t["title"]]]).to_excel(
                    writer, sheet_name=sheet, index=False, header=False)
                t["df"].to_excel(writer, sheet_name=sheet, index=False, startrow=2)
        job.update({"status": "done",
                    "tables": [{"page": t["page"], "title": t["title"], "html": t["html"]} for t in tables],
                    "count": len(tables), "excel": excel_tmp.name})
    except Exception as e:
        job.update({"status": "error", "error": str(e)})


@app.route("/")
def index():
    return HTML

@app.route("/start", methods=["POST"])
def start():
    if "pdf" not in request.files:
        return jsonify({"error": "Pas de fichier recu."})
    pdf = request.files["pdf"]
    job_id = str(uuid.uuid4())
    tmp_dir = tempfile.mkdtemp()
    pdf_path = os.path.join(tmp_dir, "input.pdf")
    out_dir = os.path.join(tmp_dir, "output")
    os.makedirs(out_dir)
    pdf.save(pdf_path)
    jobs[job_id] = {"status": "running", "progress": 10, "message": "Lecture",
                    "step": 1, "process": None, "tables": [], "count": 0,
                    "excel": None, "error": None}
    t = threading.Thread(target=run_extraction, args=(job_id, pdf_path, out_dir))
    t.daemon = True
    t.start()
    return jsonify({"job_id": job_id})

@app.route("/status/<job_id>")
def status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"status": "error", "error": "Job introuvable."})
    return jsonify({"status": job["status"], "progress": job.get("progress", 0),
                    "message": job.get("message", ""), "step": job.get("step", 1),
                    "tables": job.get("tables", []), "count": job.get("count", 0),
                    "error": job.get("error")})

@app.route("/cancel/<job_id>", methods=["POST"])
def cancel(job_id):
    job = jobs.get(job_id)
    if job:
        job["status"] = "cancelled"
        proc = job.get("process")
        if proc and proc.poll() is None:
            proc.terminate()
    return jsonify({"ok": True})

@app.route("/download/<job_id>")
def download(job_id):
    job = jobs.get(job_id)
    if job and job.get("excel") and os.path.exists(job["excel"]):
        return send_file(job["excel"], as_attachment=True, download_name="tableaux_itceq.xlsx")
    return "Fichier non disponible", 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860, debug=False)