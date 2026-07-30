; Script de compilación para Inno Setup
; Genera el instalador de Windows para la aplicación Liquidaciones ETL

[Setup]
AppName=Liquidaciones ETL
AppVersion=1.0.0
DefaultDirName={commonpf}\LiquidacionesETL
DefaultGroupName=Liquidaciones ETL
OutputDir=.
OutputBaseFilename=Instalador-Liquidaciones-ETL
Compression=lzma
SolidCompression=yes
PrivilegesRequired=admin

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; Binario compilado del servidor Express
Source: "dist\aud-server.exe"; DestDir: "{app}"; Flags: ignoreversion
; Lanzador silencioso de Chrome
Source: "lanzador.vbs"; DestDir: "{app}"; Flags: ignoreversion
; Archivos estáticos del frontend
Source: "public\*"; DestDir: "{app}\public"; Flags: ignoreversion recursesubdirs createallsubdirs
; Archivos CSV de parámetros iniciales
Source: "configuracion_parametros\*"; DestDir: "{app}\configuracion_parametros"; Flags: ignoreversion recursesubdirs createallsubdirs

[Dirs]
; Crear carpetas de trabajo del proceso ETL vacías en el cliente
Name: "{app}\excel-a-convertir"
Name: "{app}\excel-a-convertir-novedades"
Name: "{app}\csv-convertido"
Name: "{app}\csv-unidos"
Name: "{app}\csv-unidos-novedades"
Name: "{app}\uploads"

[Icons]
; Crear accesos directos apuntando al lanzador y estableciendo la carpeta raíz como WorkingDir
Name: "{group}\Liquidaciones ETL"; Filename: "{app}\lanzador.vbs"; WorkingDir: "{app}"
Name: "{commondesktop}\Liquidaciones ETL"; Filename: "{app}\lanzador.vbs"; WorkingDir: "{app}"; Tasks: desktopicon

[Run]
; Iniciar la aplicación de manera segura al terminar la instalación usando wscript
Filename: "wscript.exe"; Parameters: """{app}\lanzador.vbs"""; Description: "Iniciar Liquidaciones ETL"; Flags: postinstall nowait
