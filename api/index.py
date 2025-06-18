from flask import Flask, request, jsonify
from recommender import Recommender

app = Flask(__name__)

recommender_instance = Recommender(
    matkul_path='data/matkul.csv',
    prasyarat_path='data/prasyarat.csv',
    karier_path='data/karier.csv',
)

@app.route('/rekomendasi', methods=['POST'])
def handle_rekomendasi():
    data_klien = request.get_json()

    if not data_klien:
        return jsonify({"error": "Request body harus berupa JSON"}), 400

    nim = data_klien.get('nim')
    prodi = data_klien.get('prodi')
    ipk = data_klien.get('ipk')
    riwayat_list = data_klien.get('riwayat')
    target_semester = data_klien.get('target_semester')

    if not all([nim, prodi, ipk, riwayat_list, target_semester]):
        return jsonify({"error": "Data tidak lengkap. Field yang dibutuhkan: nim, prodi, ipk, riwayat, target_semester"}), 400

    try:
        hasil = recommender_instance.dapatkan_rekomendasi(
            nim=nim,
            prodi=prodi,
            ipk=float(ipk),
            riwayat_list=riwayat_list,
            target_semester=int(target_semester)
        )
        return jsonify(hasil)
    except Exception as e:
        print(f"Terjadi error: {e}")
        return jsonify({"error": "Terjadi kesalahan di server"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)