:: update_data.bat
:: Runs download_data_updates.py every Mon, Tue, Wed at 6pm Oregon time (Pacific Time)

:: To schedule this, use Windows Task Scheduler:
:: 1. Open Task Scheduler.
:: 2. Create a new task.
:: 3. Set "Trigger" to Weekly, select Monday, Tuesday, Wednesday, set time to 6:00 PM.
:: 4. Set "Action" to "Start a program", and point to this batch file.
:: 5. Save.

:: This batch file runs your Python script.
"C:\Users\grego\AppData\Local\Microsoft\WindowsApps\python3.13.exe" "%~dp0process_a_a_data.py" > "%~dp0update_data_log.txt" 2>&1