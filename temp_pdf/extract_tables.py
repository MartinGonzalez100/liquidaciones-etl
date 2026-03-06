import pdfplumber

with pdfplumber.open('c:\\proyecto-aud\\informe_modelo.pdf') as pdf:
    for i, page in enumerate(pdf.pages):
        print(f"--- PAGE {i+1} ---")
        tables = page.extract_tables()
        for t_idx, table in enumerate(tables):
            print(f"Table {t_idx+1}:")
            for row in table:
                print(row)
        print("\n")
