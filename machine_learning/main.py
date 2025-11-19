
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.inspection import permutation_importance
import matplotlib.pyplot as plt
import joblib

# ==============================================================
# DATASET LOADING
# ==============================================================
df = pd.read_csv("Flight-delay_dataset_post_traitement.csv")

print("============= Aperçu du dataset =============")
print(df.head(10))
df.info()

# ==============================================================
# EXPLANATORY & TARGET VARIABLES
# ==============================================================
X = df.drop('DS_FINAL_DELAY_MIN', axis=1)
y = df['DS_FINAL_DELAY_MIN']

# ==============================================================
# CATEGORIAL COLUMNS TO ENCODE
# ==============================================================
categorical_cols = [
    'DS_AIRLINE_CODE',
    'DS_DEPARTURE_AIRPORT_CODE',
    'DS_ARRIVAL_AIRPORT_CODE'
]

numeric_cols = [col for col in X.columns if col not in categorical_cols]

# ==============================================================
# PIPELINE W/ ONE-HOT ENCODING + RANDOM FOREST
# ==============================================================

# COL PROCESSING : Processing of categorial columns (non numeric)
preprocessor = ColumnTransformer(
    transformers=[
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_cols)
    ],
    remainder='passthrough'
)

# MODEL TYPE : Random Forest
regressor = RandomForestRegressor(
    n_estimators=300,       # nombre d’arbres
    max_depth=None,         # profondeur libre
    min_samples_split=10,   # taille minimale pour une division
    min_samples_leaf=2,     # taille minimale d’une feuille
    n_jobs=-1,              # utilise tous les cœurs CPU
    random_state=42
)

# PIPELINE COMPLETE
model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('regressor', regressor)
])

# ==============================================================
# SPLIT TRAIN / TEST
# ==============================================================
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=65)

# ==============================================================
# TRAINING PIPELINE
# ==============================================================
print("\n===== Entraînement du modèle (One-Hot Encoding + Random Forest) =====")
model.fit(X_train, y_train)

# ==============================================================
# MODEL EVALUATION
# ==============================================================
y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("\n===== Résultats =====")
print(f"Erreur MAE moyenne absolue : {mae:.2f} minutes")
print(f"Coefficient de détermination R² : {r2:.3f}")

# ==============================================================
# WEIGHT OF VARIABLES
# ==============================================================

print("\nCalcul des importances des variables (Permutation Importance)...")

# On calcule l'importance sur les colonnes AVANT transformation
result = permutation_importance(
    model, X_test, y_test,
    n_repeats=10,
    random_state=42,
    n_jobs=-1
)

original_feature_names = X.columns.tolist()

feature_importance_df = pd.DataFrame({
    'Variable': original_feature_names,
    'Importance': result.importances_mean
}).sort_values(by='Importance', ascending=False)

print("\n============= Importance des variables (Permutation sur variables originales) =============")
print(feature_importance_df.head(15))


#=====================================================================
# EXPORT
#=====================================================================
joblib.dump(model, "flight_delay_pipeline.joblib")
print("Pipeline complet sauvegardé sous 'flight_delay_pipeline.joblib'")


# ==============================================================
# DATA VISUALIZATION
# ==============================================================

'''
#FIGURE 1 : PREDICTION VS REAL VALUES

plt.figure(figsize=(8,6))
plt.scatter(y_test, y_pred, alpha=0.5, label='Prédictions')
# Ligne parfaite y = x
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2, label='Ideal prediction')

# Bande ±MAE
plt.plot([y_test.min(), y_test.max()],
         [y_test.min() + mae, y_test.max() + mae],
         'k--', lw=1.5, label=f'+MAE ({mae:.2f} min)')
plt.plot([y_test.min(), y_test.max()],
         [y_test.min() - mae, y_test.max() - mae],
         'k--', lw=1.5, label=f'-MAE ({mae:.2f} min)')

plt.fill_between(
    [y_test.min(), y_test.max()],
    [y_test.min() - mae, y_test.max() - mae],
    [y_test.min() + mae, y_test.max() + mae],
    color='grey', alpha=0.1, label='Zone ±MAE'
)

plt.xlabel("Real values (min)")
plt.ylabel("Predicted values (min)")
plt.title(f"Random Forest + One-Hot Encoding")
plt.legend()
plt.grid(True)
plt.show(block='False')


# FIGURE 2 : IMPACT OF EXPLANATORY VARIABLES HISTOGRAM

plt.figure(figsize=(10,6))
plt.barh(feature_importance_df['Variable'][:15], feature_importance_df['Importance'][:15])
plt.xlabel("Average importance (Permutation)")
plt.ylabel("Origin variables")
plt.title("Importance of variables — Random Forest (Permutation on original vairables)")
plt.gca().invert_yaxis()
plt.grid(True, axis='x', linestyle='--', alpha=0.6)
plt.tight_layout()  
plt.show(block=False)


# FIGURE 3 : IMPACT OF EXPLANATORY VARIABLES GRAPH

from sklearn.inspection import PartialDependenceDisplay
import matplotlib.pyplot as plt
features_to_plot = [
    'DS_PREV_DELAY_MIN',
    'DS_FLIGHT_DURATION_MIN',
    'DS_DEPARTURE_AIRPORT_RATING_NORM',
    'DS_AIRLINE_RATING_NORM'
]
fig, ax = plt.subplots(2, 2, figsize=(12, 8))  
PartialDependenceDisplay.from_estimator(
    model,
    X,
    features_to_plot,
    kind='average',
    grid_resolution=50,
    ax=ax  
)
plt.suptitle("Average effect of each variables on final delay prediction (Random Forest)", fontsize=14)
plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.show(block='False')




# FIGURE 4  : RELATION BETWEEN FINAL DELAY AND DIFFERENTS  EXPLANATORY VARIABLES


fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("Relation between final delay and differents explanatory variables (Dataset)", fontsize=16, fontweight='bold')

# Previous flight
x = df['DS_FINAL_DELAY_MIN']
y = df['DS_PREV_DELAY_MIN']
m, b = np.polyfit(x, y, 1)
axes[0, 0].scatter(x, y, color='blue', alpha=0.5, label='Data of previous flight')
#axes[0, 0].plot(x, m*x + b, color='red', label='Linear trend')
axes[0, 0].set_title("Final delay vs previous delay")
axes[0, 0].set_xlabel("Final delay (min)")
axes[0, 0].set_ylabel("Previous delay (min)")
axes[0, 0].grid(True)
axes[0, 0].legend()

# Flight delay 
x = df['DS_FINAL_DELAY_MIN']
y = df['DS_FLIGHT_DURATION_MIN']
m, b = np.polyfit(x, y, 1)
axes[0, 1].scatter(x, y, color='green', alpha=0.5, label='Durée de vol')
#axes[0, 1].plot(x, m*x + b, color='red', label='Tendance linéaire')
axes[0, 1].set_title("Retard final vs Durée de vol")
axes[0, 1].set_xlabel("Retard final (min)")
axes[0, 1].set_ylabel("Durée de vol (min)")
axes[0, 1].grid(True)
axes[0, 1].legend()

# Rating normalized of departure airport
x = df['DS_FINAL_DELAY_MIN']
y = df['DS_DEPARTURE_AIRPORT_RATING_NORM']
m, b = np.polyfit(x, y, 1)
axes[1, 0].scatter(x, y, color='purple', alpha=0.5, label="Departure airport")
#axes[1, 0].plot(x, m*x + b, color='red', label='Linéaire trend')
axes[1, 0].set_title("Final delay vs departure airport rating")
axes[1, 0].set_xlabel("Final delay (min)")
axes[1, 0].set_ylabel("Rating normalized departure airport")
axes[1, 0].grid(True)
axes[1, 0].legend()

# Rating normalized of airline
x = df['DS_FINAL_DELAY_MIN']
y = df['DS_AIRLINE_RATING_NORM']
m, b = np.polyfit(x, y, 1)
axes[1, 1].scatter(x, y, color='orange', alpha=0.5, label='Note compagnie (normalisé)')
#axes[1, 1].plot(x, m*x + b, color='red', label='Linéaire trend')
axes[1, 1].set_title("Final delay vs Rating airline")
axes[1, 1].set_xlabel("Final delay (min)")
axes[1, 1].set_ylabel("Rating normalized airline")
axes[1, 1].grid(True)
axes[1, 1].legend()

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.show()

'''
