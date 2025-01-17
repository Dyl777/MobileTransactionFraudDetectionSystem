import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA



# Load additional datasets
social_media_logs = pd.read_csv('SocialMediaLogs.csv')
accounts = pd.read_csv('Accounts.csv')
subscribers = pd.read_csv('Subscribers.csv')
transactions = pd.read_csv('Transactions.csv')

# Set the style for plots
sns.set(style="whitegrid")

# 1. **Social Media Activity vs. User Age**
plt.figure(figsize=(10, 6))
sns.histplot(
    data=social_media_logs, 
    x='User_age', 
    hue='PostType', 
    multiple='stack', 
    bins=20, 
    palette='viridis'
)
plt.title('Social Media Activity by User Age')
plt.xlabel('User Age')
plt.ylabel('Post Count')
plt.legend(title='Post Type')
plt.show()
plt.savefig("Social Media Activity by User Age")
plt.close()

# 2. **Accounts by User Age**
plt.figure(figsize=(10, 6))
sns.countplot(
    data=accounts, 
    x='Account_user_age_group', 
    hue='Account_status', 
    palette='Set2'
)
plt.title('Accounts by User Age Group')
plt.xlabel('User Age Group')
plt.ylabel('Account Count')
plt.xticks(rotation=45)
plt.legend(title='Account Status')
plt.show()
plt.savefig("AccountsbyUserAgeGroup.png")
plt.close()

# 3. **Subscribers by User Age**
plt.figure(figsize=(10, 6))
sns.histplot(
    data=subscribers, 
    x='Subscriber_user_age', 
    hue='Subscriber_type', 
    multiple='stack', 
    bins=20, 
    palette='coolwarm'
)
plt.title('Subscribers by User Age')
plt.xlabel('User Age')
plt.ylabel('Subscriber Count')
plt.legend(title='Subscriber Type')
plt.show()

# 4. **Transactions Per Region**
plt.figure(figsize=(10, 6))
sns.barplot(
    data=transactions.groupby('Region').size().reset_index(name='Transaction Count'),
    x='Region', 
    y='Transaction Count', 
    palette='Blues_d'
)
plt.title('Transactions Per Region')
plt.xlabel('Region')
plt.ylabel('Transaction Count')
plt.xticks(rotation=45)
plt.show()
plt.savefig("Transactions_vs_RegionAtLarge.png")
plt.close()

# 5. **Transactions Per City**
plt.figure(figsize=(10, 6))
sns.barplot(
    data=transactions.groupby('City').size().reset_index(name='Transaction Count'),
    x='City', 
    y='Transaction Count', 
    palette='Oranges_d'
)
plt.title('Transactions Per City')
plt.xlabel('City')
plt.ylabel('Transaction Count')
plt.xticks(rotation=45)
plt.show()
plt.savefig("Transactions_vs_City.png")
plt.close()

# 1. **PostType vs. User Age (Social Media Logs)**
plt.figure(figsize=(10, 6))
sns.histplot(
    data=social_media_logs, 
    x='User_age', 
    hue='PostType', 
    multiple='stack', 
    bins=20, 
    palette='viridis'
)
plt.title('PostType Distribution by User Age')
plt.xlabel('User Age')
plt.ylabel('Number of Posts')
plt.legend(title='Post Type')

# Save plot to file
plt.savefig("PostType_vs_UserAge.png")
plt.close()

# Save processed social media data for further analysis
social_media_logs.to_csv("SocialMediaLogs_Processed.csv", index=False)



# Load data
data = pd.read_csv('Transactions.csv')

# Add a "fraud_label" column (if not already present) for demonstration purposes
# Assuming a threshold on 'Anomaly_score' to classify fraud (1 = Fraud, 0 = Not Fraud)
# Replace this with the actual ground truth if available.
data['fraud_label'] = (data['Anomaly_score'] > 0.8).astype(int)

# Select features for training
features = data[['Transaction_amount', 'Anomaly_score']]  # Add other features as needed
labels = data['fraud_label']

# Split data into train, validation, and test sets
X_train, X_temp, y_train, y_temp = train_test_split(features, labels, test_size=0.4, random_state=42)
X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)

# Scale the features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_val_scaled = scaler.transform(X_val)
X_test_scaled = scaler.transform(X_test)

# Convert scaled data back to DataFrame for easier handling
X_train = pd.DataFrame(X_train_scaled, columns=features.columns)
X_val = pd.DataFrame(X_val_scaled, columns=features.columns)
X_test = pd.DataFrame(X_test_scaled, columns=features.columns)

# Dimensionality reduction for visualization
pca = PCA(n_components=2)
features_2d = pca.fit_transform(X_train_scaled)

# Add 2D PCA results to a DataFrame for easier plotting
plot_data = pd.DataFrame(features_2d, columns=['PCA1', 'PCA2'])

# Ensure data dimensions
assert X_train.shape[0] == y_train.shape[0]
assert X_val.shape[0] == y_val.shape[0]
assert X_test.shape[0] == y_test.shape[0]

# Train DBSCAN
dbscan = DBSCAN(eps=0.5, min_samples=5)
dbscan.fit(X_train)
plot_data['DBSCAN_Cluster'] = dbscan.fit_predict(X_train_scaled)

plt.figure(figsize=(10, 6))
sns.scatterplot(
    data=plot_data, 
    x='PCA1', 
    y='PCA2', 
    hue='DBSCAN_Cluster', 
    palette='tab10', 
    legend='full'
)
plt.title("DBSCAN Clustering Visualization")
plt.xlabel("PCA Component 1")
plt.ylabel("PCA Component 2")
plt.legend(title="Clusters")
plt.savefig("DBSCAN_Visualization.png")
plt.show()

# DBSCAN Predictions
dbscan_labels = dbscan.fit_predict(X_test)  # -1 = anomaly, 0+ = cluster
dbscan_pred = (dbscan_labels == -1).astype(int)  # Convert -1 (anomalies) to 1 (fraud)

# Train Isolation Forest
isolation_forest = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
isolation_forest.fit(X_train)

# Isolation Forest Predictions
isolation_forest_pred = isolation_forest.predict(X_test)  # -1 = anomaly, 1 = normal
isolation_forest_pred = (isolation_forest_pred == -1).astype(int)  # Convert -1 to 1 for fraud

# Evaluate DBSCAN
print("DBSCAN Metrics:")
print("Confusion Matrix:")
print(confusion_matrix(y_test, dbscan_pred))
print("Classification Report:")
print(classification_report(y_test, dbscan_pred, target_names=['Not Fraud', 'Fraud']))
print(f"ROC-AUC Score: {roc_auc_score(y_test, dbscan_pred):.2f}")

# Evaluate Isolation Forest
print("\nIsolation Forest Metrics:")
print("Confusion Matrix:")
print(confusion_matrix(y_test, isolation_forest_pred))
print("Classification Report:")
print(classification_report(y_test, isolation_forest_pred, target_names=['Not Fraud', 'Fraud']))
print(f"ROC-AUC Score: {roc_auc_score(y_test, isolation_forest_pred):.2f}")