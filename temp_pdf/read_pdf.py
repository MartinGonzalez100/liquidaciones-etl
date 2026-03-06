import PyPDF2

try:
    with open('../informe_modelo.pdf', 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        for i, page in enumerate(reader.pages):
            print(f"--- PAGE {i+1} ---")
            print(page.extract_text())
except Exception as e:
    print(f"Error: {e}")
