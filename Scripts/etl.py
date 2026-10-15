import pandas as pd

wine = pd.read_csv("data/winequality.csv")

print(wine.head())
print(wine.isnull().sum())
