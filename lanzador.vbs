Set WshShell = CreateObject("WScript.Shell")

' 1. Iniciar el servidor Express en segundo plano (el parámetro 0 oculta la consola de comandos)
WshShell.Run "aud-server.exe", 0, False

' 2. Esperar 1.5 segundos para que el servidor Express levante
WScript.Sleep 1500

' 3. Ejecutar Google Chrome en modo aplicación (sin pestañas ni barra de direcciones)
On Error Resume Next
WshShell.Run "chrome.exe --app=http://localhost:3000", 1, False

' 4. Fallback: Si Chrome no está disponible, abrir en el navegador predeterminado
If Err.Number <> 0 Then
    WshShell.Run "http://localhost:3000", 1, False
End If
