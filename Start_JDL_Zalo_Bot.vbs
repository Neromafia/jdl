' Script khoi dong ngam Zalo Bot cho Kho JDL khong hien cua so den (Windowless)
Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "c:\Users\Admin\Desktop\JDL Dashboard"
WshShell.Run """C:\Users\Admin\AppData\Local\Programs\Python\Python312\pythonw.exe"" ""c:\Users\Admin\Desktop\JDL Dashboard\zalo_bot_service.py""", 0, False
