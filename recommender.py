import pandas as pd
from rdflib import Graph, Literal, RDF, RDFS, URIRef, Namespace
from rdflib.namespace import FOAF, XSD
from datetime import datetime
from itertools import combinations

STUDENT_NS = Namespace("http://example.org/mahasiswa/")
COURSE_NS = Namespace("http://example.org/matakuliah/")
KARIER_NS = Namespace("http://example.org/karier/")
SCHEMA_NS = Namespace("http://schema.org/")

memiliki_sks = SCHEMA_NS.numberOfCredits
memiliki_prodi_mk = COURSE_NS.programStudiMK
memiliki_prodi_mhs = STUDENT_NS.programStudiMHS
memiliki_jadwal_hari = SCHEMA_NS.dayOfWeek
memiliki_jadwal_jam_mulai = SCHEMA_NS.startTime
memiliki_jadwal_jam_selesai = SCHEMA_NS.endTime
telah_mengambil_mk = STUDENT_NS.telahMengambilMK
memiliki_nilai = STUDENT_NS.memilikiNilai
memiliki_prasyarat = COURSE_NS.memilikiPrasyarat
belajar_di_semester_mhs = STUDENT_NS.belajarDiSemesterMHS
memiliki_sifat_mk = COURSE_NS.memilikiSifatMK
semester_penawaran_mk = COURSE_NS.semesterPenawaran
mendukung_karier = KARIER_NS.mendukungKarier
NILAI_LULUS = {'A', 'AB', 'B', 'BC', 'C'}

class Recommender:
    def __init__(self, matkul_path, prasyarat_path, karier_path):
        print("Menginisialisasi Recommender...")
        self.df_matkul = self._muat_dan_bersihkan_data(matkul_path)
        df_prasyarat = self._muat_dan_bersihkan_data(prasyarat_path)
        df_karier = self._muat_dan_bersihkan_data(karier_path)
        
        self.g_dasar = self._bangun_kg_dasar(df_prasyarat, df_karier)
        self.uri_ke_kode_map = {self._uri_matakuliah(row['kode_matkul']): row['kode_matkul'] for _, row in self.df_matkul.iterrows()}
        print("Recommender siap digunakan.")

    def _uri_matakuliah(self, kode_mk):
        if pd.isna(kode_mk): return COURSE_NS[f"unknown_mk_{hash(str(kode_mk))}"]
        return COURSE_NS[str(kode_mk).replace(" ", "_").replace("-", "_").replace("/", "_")]

    def _uri_mahasiswa(self, nim):
        return STUDENT_NS[str(nim)]
        
    def _muat_dan_bersihkan_data(self, path):
        dtype_options = {'nim': str} if 'nim' in pd.read_csv(path, nrows=1).columns else None
        df = pd.read_csv(path, encoding='utf-8', dtype=dtype_options)
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()
        return df

    def _bangun_kg_dasar(self, df_prasyarat, df_karier):
        print("Membangun Knowledge Graph dasar...")
        g = Graph()
        for _, baris in self.df_matkul.iterrows():
            mk_uri = self._uri_matakuliah(baris['kode_matkul'])
            g.add((mk_uri, RDF.type, COURSE_NS.MataKuliah))
            g.add((mk_uri, FOAF.name, Literal(baris['nama_matkul'], lang="id")))
            g.add((mk_uri, memiliki_sks, Literal(baris['sks_matkul'], datatype=XSD.integer)))
            if 'sifat_mk' in self.df_matkul.columns: g.add((mk_uri, memiliki_sifat_mk, Literal(str(baris['sifat_mk']))))
            if 'semester_matkul' in self.df_matkul.columns: g.add((mk_uri, semester_penawaran_mk, Literal(str(baris['semester_matkul']))))
            if pd.notna(baris['prodi_matkul']): g.add((mk_uri, memiliki_prodi_mk, Literal(baris['prodi_matkul'])))
            if pd.notna(baris['hari']): g.add((mk_uri, memiliki_jadwal_hari, Literal(baris['hari'])))
            if pd.notna(baris['jam_mulai']): g.add((mk_uri, memiliki_jadwal_jam_mulai, Literal(baris['jam_mulai'])))
            if pd.notna(baris['jam_selesai']): g.add((mk_uri, memiliki_jadwal_jam_selesai, Literal(baris['jam_selesai'])))
        
        for _, baris in df_prasyarat.iterrows():
            if pd.notna(baris['kode_matkul_relevan']) and pd.notna(baris['kode_matkul']):
                mk_utama_uri = self._uri_matakuliah(baris['kode_matkul_relevan'])
                mk_prasyarat_uri = self._uri_matakuliah(baris['kode_matkul'])
                if (mk_utama_uri, RDF.type, COURSE_NS.MataKuliah) in g and (mk_prasyarat_uri, RDF.type, COURSE_NS.MataKuliah) in g:
                    g.add((mk_utama_uri, memiliki_prasyarat, mk_prasyarat_uri))
        
        for _, baris in df_karier.iterrows():
            mk_uri = self._uri_matakuliah(baris['kode_matkul'])
            if (mk_uri, RDF.type, COURSE_NS.MataKuliah) in g:
                karier_str = baris['relevansi_karier']
                karier_uri = KARIER_NS[karier_str.replace(" ", "_")]
                g.add((mk_uri, mendukung_karier, karier_uri))
                g.add((karier_uri, RDF.type, KARIER_NS.RelevansiKareir))
                g.add((karier_uri, RDFS.label, Literal(karier_str)))
        
        print(f"KG Dasar selesai dibangun dengan {len(g)} triples.")
        return g

    def _get_mk_telah_diambil(self, g, nim_str):
        mhs_uri_target = self._uri_mahasiswa(nim_str)
        mk_diambil = {}
        for _, _, kejadian_uri in g.triples((mhs_uri_target, telah_mengambil_mk, None)):
            mk_uri_diambil = g.value(subject=kejadian_uri, predicate=STUDENT_NS.mataKuliah)
            nilai_literal = g.value(subject=kejadian_uri, predicate=memiliki_nilai)
            if mk_uri_diambil and nilai_literal:
                kode_mk_asli = self.uri_ke_kode_map.get(mk_uri_diambil)
                if kode_mk_asli: mk_diambil[kode_mk_asli] = str(nilai_literal)
        return mk_diambil

    def _cek_prasyarat_terpenuhi(self, riwayat_mahasiswa, mk_target_uri):
        for _, _, prasyarat_uri in self.g_dasar.triples((mk_target_uri, memiliki_prasyarat, None)):
            kode_prasyarat_asli = self.uri_ke_kode_map.get(prasyarat_uri)
            if not kode_prasyarat_asli or not (kode_prasyarat_asli in riwayat_mahasiswa and riwayat_mahasiswa[kode_prasyarat_asli] in NILAI_LULUS):
                return False
        return True
    
    def _parse_waktu(self, waktu_str):
        try: return datetime.strptime(str(waktu_str), '%I:%M:%S %p').time()
        except (ValueError, TypeError):
            try: return datetime.strptime(str(waktu_str), '%H:%M:%S').time()
            except (ValueError, TypeError): return None

    def _cek_jadwal_bentrok(self, list_kode_mk):
        if len(list_kode_mk) < 2: return False
        jadwal_mk = []
        for kode_mk in list_kode_mk:
            detail_mk = self.df_matkul[self.df_matkul['kode_matkul'] == kode_mk].iloc[0]
            jam_mulai, jam_selesai = self._parse_waktu(detail_mk['jam_mulai']), self._parse_waktu(detail_mk['jam_selesai'])
            if jam_mulai and jam_selesai:
                jadwal_mk.append({'kode': kode_mk, 'hari': detail_mk['hari'], 'mulai': jam_mulai, 'selesai': jam_selesai})
        for mk1, mk2 in combinations(jadwal_mk, 2):
            if mk1['hari'] == mk2['hari'] and (mk1['mulai'] < mk2['selesai'] and mk2['mulai'] < mk1['selesai']):
                return True
        return False

    def _konversi_nilai_ke_skor(self, nilai_str):
        peta_nilai = {'A': 4.0, 'AB': 3.5, 'B': 3.0, 'BC': 2.5, 'C': 2.0}
        return peta_nilai.get(str(nilai_str).upper(), 0.0)

    def dapatkan_rekomendasi(self, nim, prodi, ipk, riwayat_list, target_semester, sks_maks=None):
        riwayat_mahasiswa = {item['kode']: item['nilai'] for item in riwayat_list}
        mk_sudah_diambil = set(riwayat_mahasiswa.keys())

        batas_sks = sks_maks if sks_maks is not None else (18 if ipk <= 2.5 else 24)
        mk_belum_diambil = set(self.df_matkul['kode_matkul'].unique()) - mk_sudah_diambil

        kandidat_dengan_skor = []
        for kode_mk in mk_belum_diambil:
            data_mk = self.df_matkul[self.df_matkul['kode_matkul'] == kode_mk].iloc[0]
            if data_mk['sifat_mk'].upper() != 'PILIHAN': continue
            
            if self._cek_prasyarat_terpenuhi(riwayat_mahasiswa, self._uri_matakuliah(kode_mk)):
                total_skor, jumlah_prasyarat, prasyarat_ditemukan = 0, 0, {}
                for _, _, prasyarat_uri in self.g_dasar.triples((self._uri_matakuliah(kode_mk), memiliki_prasyarat, None)):
                    jumlah_prasyarat += 1
                    kode_prasyarat = self.uri_ke_kode_map.get(prasyarat_uri)
                    if kode_prasyarat and kode_prasyarat in riwayat_mahasiswa:
                        nilai = riwayat_mahasiswa[kode_prasyarat]
                        total_skor += self._konversi_nilai_ke_skor(nilai)
                        prasyarat_ditemukan[kode_prasyarat] = nilai
                skor_rata_rata = (total_skor / jumlah_prasyarat) if jumlah_prasyarat > 0 else 3.0
                kandidat_dengan_skor.append({'kode': kode_mk, 'nama': data_mk['nama_matkul'], 'sks': data_mk['sks_matkul'], 'prodi': data_mk['prodi_matkul'], 'skor': skor_rata_rata, 'prasyarat_terpenuhi': prasyarat_ditemukan})
        
        kandidat_terurut = sorted(kandidat_dengan_skor, key=lambda x: x['skor'], reverse=True)
        
        rekomendasi_terpilih, sks_saat_ini = [], 0
        for kandidat in kandidat_terurut:
            if sks_saat_ini + kandidat['sks'] <= batas_sks:
                kode_cek_bentrok = [mk['kode'] for mk in rekomendasi_terpilih] + [kandidat['kode']]
                if not self._cek_jadwal_bentrok(kode_cek_bentrok):
                    rekomendasi_terpilih.append(kandidat)
                    sks_saat_ini += kandidat['sks']
                    
        hasil_final = []
        for mk_info in rekomendasi_terpilih:
            penjelasan = "Direkomendasikan sebagai mata kuliah pilihan yang relevan dan tidak memiliki prasyarat."
            if mk_info['prasyarat_terpenuhi']:
                list_prasyarat = [f"{self.df_matkul[self.df_matkul['kode_matkul'] == k]['nama_matkul'].iloc[0]} (Nilai: {v})" for k, v in mk_info['prasyarat_terpenuhi'].items()]
                penjelasan = f"Direkomendasikan karena Anda berprestasi baik pada mata kuliah prasyarat: {', '.join(list_prasyarat)}."
            list_karier = []
            for _, _, karier_uri in self.g_dasar.triples((self._uri_matakuliah(mk_info['kode']), mendukung_karier, None)):
                nama_karier = self.g_dasar.value(subject=karier_uri, predicate=RDFS.label)
                if nama_karier:
                    list_karier.append(str(nama_karier))
            hasil_final.append({
                "kode_mk": mk_info['kode'], "nama_mk": mk_info['nama'], "sks": int(mk_info['sks']),
                "prodi_penawar": mk_info['prodi'], "skor": round(mk_info['skor'], 2), "alasan": penjelasan,
                "relevansi_karier": list_karier
            })
            
        return {"total_sks_rekomendasi": int(sks_saat_ini), "rekomendasi": hasil_final}