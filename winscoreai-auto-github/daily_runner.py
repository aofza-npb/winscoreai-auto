# daily_runner.py

import os
import sys
import traceback
import schedule
import time
from datetime import datetime
from win_data import generate_win_data
from predictor import run_prediction

# 🧠 Import ฟังก์ชันหลักจากแต่ละระบบ
from main import job as run_understat_scraper


def run_all():
    print("📅 เริ่มต้นระบบวิเคราะห์ WinScoreAI –", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    try:
        print("\n🟡 ขั้นตอนที่ 1: ดึงข้อมูลจาก Understat (วันละลีก)...")
        run_understat_scraper()

        print("\n🟡 ขั้นตอนที่ 2: ประมวลผลฟอร์มทีม (win_data.csv)...")
        generate_win_data()

        print("\n🟡 ขั้นตอนที่ 3: วิเคราะห์ผลการแข่งขัน (predict_result.csv) และส่งเข้า Firebase...")
        run_prediction()

        print("\n✅ เสร็จสมบูรณ์ทุกขั้นตอน 🎉 WinScoreAI พร้อมใช้งาน!")

    except Exception as e:
        print("\n❌ เกิดข้อผิดพลาดในการทำงาน:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_all()

schedule.every().day.at("09:00").do(run_all)

while True:
    schedule.run_pending()
    time.sleep(60)
