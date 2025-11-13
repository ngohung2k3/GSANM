import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import pickle
import os
import re
from imblearn.over_sampling import SMOTE
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import urllib.parse
custom_stop_words = ENGLISH_STOP_WORDS.copy()
words_to_keep = {'or', 'and', 'select', 'union', 'insert', 'delete', 'update', 'drop'}
custom_stop_words = custom_stop_words - words_to_keep
custom_stop_words = list(custom_stop_words)
def convert_label(attack_type):
	label_mapping = {'norm': 0, 'sqli': 1, 'xss': 2}
	return label_mapping.get(attack_type, -1)
def decode_url(encoded_url):
	encoded_url = encoded_url.lower()
	encoded_url = encoded_url.strip()
	encoded_url = re.sub(r'\s+', ' ', encoded_url)
	decoded_url = urllib.parse.unquote(encoded_url)
	return decoded_url
def train_rf_model(csv_path, model_save_path, vectorizer_save_path,test_size=0.2, random_state=42):
	print("Đọc dữ liệu từ tệp CSV...")
	data = pd.read_csv(csv_path, usecols=['payload', 'attack_type'])
	data = data.sample(n=10000, random_state=42)
	print(f"Số lượng mẫu ban đầu: {len(data)}")
	print("Tiền xử lý payload...")
	data['payload'] = data['payload'].astype(str).apply(decode_url)
	print("Chuyển đổi nhãn thành số...")
	data['Label'] = data['attack_type'].apply(convert_label)
	initial_length = len(data)
	data = data[data['Label'] != -1]
	print(f"Đã loại bỏ {initial_length - len(data)} mẫu không xác định.")
	print(f"Số lượng mẫu sau khi loại bỏ: {len(data)}")
	X = data['payload']
	y = data['Label']
	print("Huấn luyện TfidfVectorizer...")
	vectorizer = TfidfVectorizer(
		max_features=5000,
		ngram_range=(1,2),
		min_df=2,
		max_df=0.95,
		stop_words=custom_stop_words
	)
	X_tfidf = vectorizer.fit_transform(X)
	print(f"Kích thước ma trận TF-IDF: {X_tfidf.shape}")
	print("Over-sampling các lớp thiểu số với SMOTE...")
	smote = SMOTE(random_state=random_state)
	X_resampled, y_resampled = smote.fit_resample(X_tfidf, y)
	print(f"Phân phối lớp sau SMOTE: {pd.Series(y_resampled).value_counts()}")
	print("Chia dữ liệu thành tập huấn luyện và tập kiểm tra...")
	X_train, X_test, y_train, y_test = train_test_split(
		X_resampled, y_resampled, test_size=test_size,random_state=random_state, stratify=y_resampled)
	print(f"Số lượng mẫu huấn luyện: {X_train.shape[0]}")
	print(f"Số lượng mẫu kiểm tra: {X_test.shape[0]}")
	print("Huấn luyện mô hình Random Forest...")
	rf = RandomForestClassifier(n_estimators=100, random_state=random_state)
	rf.fit(X_train, y_train)
	print("Mô hình Random Forest đã được huấn luyện.")
	print("Đánh giá mô hình trên tập kiểm tra...")
	y_pred = rf.predict(X_test)
	accuracy = accuracy_score(y_test, y_pred)
	print(f"Độ chính xác (Accuracy): {accuracy:.4f}")
	print("Báo cáo phân loại:")
	print(classification_report(y_test, y_pred,target_names=['norm', 'sqli', 'xss']))
	print("Ma trận nhầm lẫn:")
	print(confusion_matrix(y_test, y_pred))
	print(f"Lưu mô hình Random Forest vào '{model_save_path}'...")
	with open(model_save_path, 'wb') as model_file:
		pickle.dump(rf, model_file)
	print(f"Lưu vectorizer vào '{vectorizer_save_path}'...")
	with open(vectorizer_save_path, 'wb') as vectorizer_file:
		pickle.dump(vectorizer, vectorizer_file)
	print("Huấn luyện và lưu mô hình hoàn tất.")
if __name__ == "__main__":
	CSV_PATH ='../HttpParamsDataset/payload_full.csv'
	MODEL_SAVE_PATH = './rf_model.pkl'
	VECTORIZER_SAVE_PATH = './vectorizer_rf.pkl'
	
	os.makedirs(os.path.dirname(MODEL_SAVE_PATH), exist_ok=True)
	train_rf_model(
		csv_path=CSV_PATH,
		model_save_path=MODEL_SAVE_PATH,
	vectorizer_save_path=VECTORIZER_SAVE_PATH,
	test_size=0.2,
	random_state=42
)
