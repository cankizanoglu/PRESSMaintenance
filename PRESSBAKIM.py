import requests
import pyodbc
import sys
from datetime import datetime

class KalipBakimSistemi:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.conn = self.db_baglanti()

    def db_baglanti(self):
        try:
            conn = pyodbc.connect(
                "Driver={SQL Server};"
                "Server=192.168.1.15;"
                "Database=HTSLIFE_2018;"
                "UID=SA;"
                "PWD=;"
            )
            return conn
        except Exception as e:
            print("Veritabanına bağlanırken hata oluştu:", e)
            sys.exit(1)

    def get_pres_bilgisi(self, islem_no, stok_kodu):
        """
        TREX_AKTARIM tablosundan bugünden itibaren olan pres bilgilerini çek
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT MakineTuru, SUM(Miktar1) as ToplamMiktar, StokKodu
            FROM [HTSLIFE_2018].[dbo].[TREX_AKTARIM] 
            WHERE StokKodu = ? 
            AND CONVERT(DATE, IslemBaslamaTarihi) = CONVERT(DATE, GETDATE())
            GROUP BY MakineTuru, StokKodu
        """, (stok_kodu,))
        
        row = cursor.fetchone()
        if row:
            makine_turu, miktar, stok_kodu = row
            return {
                "makine_turu": makine_turu,
                "basim_sayisi": float(miktar) if miktar else 0,
                "stok_kodu": stok_kodu
            }
        else:
            print(f"Stok Kodu {stok_kodu} için bugün yapılan kayıt bulunamadı.")
            return None

    def get_bakim_sayaci(self, stok_kodu):
        """
        PRES_BAKIM_TAKIP tablosundan bakım sayacını al
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT BasimSayaci, SonBakimTarihi 
            FROM PRES_BAKIM_TAKIP 
            WHERE StokKodu = ?
        """, (stok_kodu,))
        
        row = cursor.fetchone()
        if not row:
            # Eğer kayıt yoksa yeni kayıt oluştur
            cursor.execute("""
                INSERT INTO PRES_BAKIM_TAKIP (StokKodu, BasimSayaci, SonBakimTarihi)
                VALUES (?, 0, GETDATE())
            """, (stok_kodu,))
            self.conn.commit()
            return 0, datetime.now()
        return row[0], row[1]

    def update_bakim_sayaci(self, stok_kodu, yeni_baski):
        """
        Bakım sayacını güncelle
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE PRES_BAKIM_TAKIP 
            SET BasimSayaci = BasimSayaci + ?
            WHERE StokKodu = ?
        """, (yeni_baski, stok_kodu))
        self.conn.commit()

    def bakim_sifirla(self, stok_kodu):
        """
        Bakım yapıldığında sayacı sıfırla
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE PRES_BAKIM_TAKIP 
            SET BasimSayaci = 0, SonBakimTarihi = GETDATE()
            WHERE StokKodu = ?
        """, (stok_kodu,))
        self.conn.commit()
        print(f"{stok_kodu} için bakım sayacı sıfırlandı.")

    def bakim_kontrolu(self, islem_no, stok_kodu, bakim_esik=20000):
        pres_bilgisi = self.get_pres_bilgisi(islem_no, stok_kodu)
        if not pres_bilgisi:
            return

        mevcut_sayac, son_bakim = self.get_bakim_sayaci(stok_kodu)
        yeni_baski = pres_bilgisi["basim_sayisi"]
        
        # Sayacı güncelle
        self.update_bakim_sayaci(stok_kodu, yeni_baski)
        
        # Güncel sayaç değerini al
        toplam_basim = mevcut_sayac + yeni_baski
        kalan_basim = bakim_esik - toplam_basim
        oran = (kalan_basim / bakim_esik) * 100

        print(f"Makine: {pres_bilgisi['makine_turu']}")
        print(f"Stok Kodu: {stok_kodu}")
        print(f"Toplam Basım: {toplam_basim:,.0f}")
        print(f"Son Bakım Tarihi: {son_bakim}")
        print(f"Bakım Eşiği: {bakim_esik:,.0f}")
        print(f"Kalan Basım: {kalan_basim:,.0f}")
        print(f"Kalan Oran: %{oran:.1f}")

        if kalan_basim <= bakim_esik * 0.1:
            mesaj = (f"⚠️ UYARI: {pres_bilgisi['makine_turu']} için bakım zamanı yaklaşıyor!\n"
                     f"Stok Kodu: {stok_kodu}\n"
                     f"Toplam Basım: {toplam_basim:,.0f}\n"
                     f"Son Bakım Tarihi: {son_bakim}\n"
                     f"Kalan basım: {kalan_basim:,.0f}\n"
                     f"Toplam eşik: {bakim_esik:,.0f}\n"
                     f"Kalan oran: %{oran:.1f}")
            self.telegram_mesaji_gonder(mesaj)
        else:
            print("Bakım uyarısına gerek yok.")

    def telegram_mesaji_gonder(self, mesaj):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        params = {"chat_id": self.chat_id, "text": mesaj}
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                print("Telegram mesajı başarıyla gönderildi.")
            else:
                print(f"Mesaj gönderilemedi. Hata: {response.status_code}")
        except requests.RequestException as e:
            print("Telegram mesajı gönderilirken hata oluştu:", e)

# Uygulama Kullanım Örneği:
if __name__ == "__main__":
    sistem = KalipBakimSistemi(
        token="7921684277:AAF_3jtaqBl5GhZl5nWBcSS_Rkq4SdgrZHI",
        chat_id="6580000530"
    )
    
    # Normal kontrol için:
    islem_no = '1187'
    stok_kodu = '160.0007.001'
    bakim_esik = 20000
    
    sistem.bakim_kontrolu(islem_no, stok_kodu, bakim_esik)
    
    # Bakım yapıldığında sayacı sıfırlamak için:
    # sistem.bakim_sifirla(stok_kodu)