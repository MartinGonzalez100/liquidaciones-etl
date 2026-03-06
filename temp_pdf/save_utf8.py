import pdfplumber

with pdfplumber.open("c:/proyecto-aud/informe_modelo.pdf") as pdf:
    with open("c:/proyecto-aud/temp_pdf/utf8_out.txt", "w", encoding="utf-8") as f:
        for i, page in enumerate(pdf.pages):
            f.write(f"--- PAGE {i+1} ---\n")
            text = page.extract_text()
            if text:
                f.write(text + "\n")
            f.write("\n")
