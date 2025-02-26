# **Client Segment Prediction Model**  

This project builds a **machine learning model** to predict business client segments based on transactional and demographic data. It supports **personalized engagement strategies, risk assessment, and client retention efforts** by classifying clients into predefined categories.  

---

## **Project Overview**  

| Component | Description |  
|-----------|------------|  
| **Objective** | Develop a predictive model that classifies business clients into **RFMT-based segments**. |  
| **Data Sources** | Transaction activity, firmographics, and labeled segment categories. |  
| **Techniques** | Feature engineering, classification models, hyperparameter tuning. |  
| **Models Evaluated** | Random Forest, XGBoost, LightGBM, CatBoost, Neural Networks. |  
| **Best Model** | GradientBoosting Classifier model |  

---

## **Data Sources**  

- **Business Clients Clusters** → Labeled dataset mapping clients to predefined RFMT clusters.  
- **Business Clients Demographics** → Firmographic data, including company size, industry, and location.  

### **Data Preparation & Exploration**  
1. **Data Merging** → Combined segmentation labels with demographic attributes.  
2. **Missing Value Analysis** → Identified and handled null values.  
3. **Duplicate Check** → Ensured unique client records.  
4. **Feature Engineering** → Created derived features to enhance model accuracy.  

---

## **Methodology**  

### **1️⃣ Data Preprocessing**  
- Standardized categorical variables using **Label Encoding & One-Hot Encoding**.  
- Scaled numerical features with **MinMaxScaler & StandardScaler**.  
- Applied **Principal Component Analysis (PCA)** for dimensionality reduction.  

### **2️⃣ Model Training & Evaluation**  
Trained multiple classification models, including:
- **Linear Models** → Logistic Regression, Ridge Classifier, Gaussian Naïve Bayes. 
- **Tree-based Model (Single)** → Decision Tree Classifier.
- **Bagging-Based (Parallel Ensemble)** → Random Forest Classifier, Bagging Classifier, Extra Trees Classifier.
- **Boosting-Based (Sequential Ensemble)** → Gradient Boosting Classifier, LightGBM Classifier, CatBoost Classifier, XGBoost Classifier.
- **Stacking-Based Model (Meta-Learning)** → Voting Classifier.  

### **3️⃣ Model Selection & Optimization**  
- Compared models using **accuracy, F1-score, ROC-AUC, and log loss**.  
- Performed **hyperparameter tuning** using GridSearchCV.  
- Validated results using **Stratified K-Fold Cross-Validation**.  

---

## **Results & Insights**  

| Model                          | Dataset     | Accuracy | F1 Score |  
|--------------------------------|------------|---------|----------|  
| **Gradient Boosting Classifier** | Training    | 76%    | 73%   |  
| **Gradient Boosting Classifier** | Test        | 68%  | 76%   |  

### **Additional Observations**  
- The **training accuracy (76%)** is higher than the **test accuracy (68%)**, indicating a slight generalization gap.  
- The **weighted F1 score** on the test set (76%) is slightly higher than on the training set (73%), suggesting the model balances precision and recall well.  
- **Confusion matrices and classification reports** show some misclassification, which could be further analyzed to refine model performance.  


### **Key Business Takeaways**  
- **Predictive models enable automated client segmentation at onboarding.**  
- **High-value client identification helps prioritize engagement strategies.**  
- **Segment-based targeting improves marketing effectiveness.**  

---

## **Next Steps**  

1. **Fine-Tune Best Model** → Apply feature selection and advanced hyperparameter tuning.  
2. **Deploy the Model** → Integrate into production for real-time classification.  
3. **Monitor Performance** → Set up model drift detection.  
4. **Expand Dataset** → Inbusiness additional behavioral and financial features.  

---

## **Project Requirements**  

- **Python 3.8+**  
- Key Libraries:  
  - `pandas`, `numpy`, `seaborn`, `matplotlib`, `scikit-learn`  
  - `xgboost`, `lightgbm`, `catboost`  
  - `pickle` (for model persistence)  

---

## **How to Use**  

1. **Load Data** → Run `data_preprocessing.ipynb` to clean and prepare datasets.  
2. **Train Model** → Execute `model_training.ipynb` to train and evaluate classifiers.  
3. **Deploy Model** → Use `inference.py` to make predictions on new clients.  