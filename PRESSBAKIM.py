import requests
import pyodbc  
import sys

class BakimUyarisiSistemi:
    def __init__(self, esik_degeri=5, token="7921684277:AAF_3jtaqBl5GhZl5nWBcSS_Rkq4SdgrZHI", chat_id="6580000530"):
        self.sayaç = 0
        self.esik_degeri = esik_degeri
        self.token = token
        self.chat_id = chat_id

    def db_baglanti(self):
        # SQL Server bağlantısı
        try:
            conn = pyodbc.connect(
                r"Driver={SQL Server};"
                r"Server=192.168.1.15;"  # Sunucu ve instance adı (raw string kullanıldı)
                "Database=DB NAME;"              # Veritabanı adı
                   "UID=DB UID;"   # SQL Server'daki kullanıcı adı
                   "PWD= DB PWD;"         # Windows kimlik doğrulaması
            )
            return conn
        except Exception as e:
            print("Veritabanına bağlanırken bir hata oluştu:", e)
            sys.exit(1)

    def press_sayisi_al(self):
        # Veritabanından press sayısını al
        try:
            conn = self.db_baglanti()
            cursor = conn.cursor()
            cursor.execute("SELECT press_count FROM dbo.press_sayaci WHERE id = 1")
            press_sayisi = cursor.fetchone()[0]  # Sonuç tek bir satır dönecek
            conn.close()
            return int(press_sayisi)  # Değeri int'e dönüştür
        except Exception as e:
            print("Press sayısı alınırken bir hata oluştu:", e)
            return 0  # Hata durumunda varsayılan olarak 0 döndür

    def basildiginda(self):
        # Press sayısını veritabanından al
        self.sayaç = self.press_sayisi_al()
        print(f"Basilma sayisi: {self.sayaç}")

        # Eğer sayaç belirli bir değeri geçerse, bakım mesajı gönderilir
        if self.sayaç >= self.esik_degeri:
            self.bakim_mesaji()

    def bakim_mesaji(self):
        mesaj = "Uyari: Bakim zamani geldi! Lütfen bakim yapin."
        self.telegram_mesaji_gonder(mesaj)

    def telegram_mesaji_gonder(self, mesaj):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        params = {
            "chat_id": self.chat_id,
            "text": mesaj
        }
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                print("Telegram mesajı başarıyla gönderildi.")
            else:
                print(f"Mesaj gönderilemedi. Hata: {response.status_code}")
                print("Hata Detayları:", response.text)  # Hata detaylarını yazdır
        except requests.RequestException as e:
            print("Telegram mesajı gönderilirken bir hata oluştu:", e)

# Uygulama
sistem = BakimUyarisiSistemi(
    esik_degeri=5, 
    token="7921684277:AAF_3jtaqBl5GhZl5nWBcSS_Rkq4SdgrZHI",  # Bot token'ınızı buraya yazın
    chat_id="6580000530"  # Chat ID'nizi buraya yazın
)

# Press sayısına bağlı olarak bildirim gönderme
sistem.basildiginda()
