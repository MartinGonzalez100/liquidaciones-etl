import pdfplumber

try:
    with pdfplumber.open('../informe_modelo.pdf') as pdf:
        for i, page in enumerate(pdf.pages):
            print(f"--- PAGE {i+1} ---")
            text = page.extract_text(layout=True)
            print(text)
except Exception as e:
    print(f"Error: {e}")
