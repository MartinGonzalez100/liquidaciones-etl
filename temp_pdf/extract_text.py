import pdfplumber
import sys

with pdfplumber.open("c:/proyecto-aud/informe_modelo.pdf") as pdf:
    for i, page in enumerate(pdf.pages):
        text = page.extract_text()
        print(f"--- PAGE {i+1} ---")
        if text:
            print(text.encode('utf-8').decode('utf-8', 'ignore'))
        print("\n")
