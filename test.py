import pandas as pd

# Beispiel DataFrame erstellen
data = {'Spalte_A': ['Wert1', 'Wert2', 'Wert3', 'Wert1', 'Wert4'],
        'Spalte_B': [10, 20, 30, 40, 50]}
df = pd.DataFrame(data)

# Filtern basierend auf zwei Werten in Spalte_A
filtered_df = df[(df['Spalte_A'] == 'Wert1') | (df['Spalte_A'] == 'Wert2')]

print(filtered_df)
