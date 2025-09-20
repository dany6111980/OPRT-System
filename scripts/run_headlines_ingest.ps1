$py = "$Env:LocalAppData\Programs\Python\Python313\python.exe"
& $py "C:\OPRT\scripts\headlines_ingest.py" *>> "C:\OPRT\logs\headlines_ingest.log"
