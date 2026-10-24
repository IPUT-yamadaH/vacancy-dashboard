import threading
import json
import os
import datetime
from zoneinfo import ZoneInfo
import socket
import secrets
import hmac
from flask import Flask, request, jsonify, render_template, redirect, url_for, session
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# ⚙️ 設定
# ==========================================
hostname = os.getenv("SENSOR_HOST", "0.0.0.0")
socket_port = int(os.getenv("SENSOR_PORT", "8765"))
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
VACANT_GRACE_SECONDS = 20 * 60

# 🔧 「利用中」と判定する感圧センサーのしきい値
# ※ これまでリアルタイム表示(500)と過去グラフ集計(1000)で値がズレていたのが
#    「上の座席状況」と「下のグラフ」が噛み合わない主原因の一つでした。
#    必ずこの1つの定数だけを全ロジックで参照するようにしています。
OCCUPIED_THRESHOLD = 500

JST = ZoneInfo("Asia/Tokyo")
WEEKDAY_JP = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日", "日曜日"]  # datetime.weekday() (月=0)

# 🪑 座席2〜20番は実機センサーが無いため、固定のダミーデータとして扱う
#    （リロードしても内容が変わらないよう、ここで1箇所だけ定義する）
FIXED_DUMMY_OCCUPIED_SEATS = {2, 5, 8, 11, 14, 17}

DB_CONFIG = {
    "host": os.getenv("SUPABASE_HOST", ""),
    "port": int(os.getenv("SUPABASE_PORT", "5432")),
    "database": os.getenv("SUPABASE_DATABASE", "postgres"),
    "user": os.getenv("SUPABASE_USER", "postgres"),
    "password": os.getenv("SUPABASE_PASSWORD", ""),
    "sslmode": os.getenv("SUPABASE_SSLMODE", "require"),
}


def get_db_connection():
    missing = [key for key in ("host", "password") if not DB_CONFIG[key]]
    if missing:
        raise RuntimeError("Supabase接続設定が不足しています。環境変数を設定してください。")
    return psycopg2.connect(**DB_CONFIG)

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY") or secrets.token_hex(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.getenv("USE_HTTPS", "false").lower() in {"1", "true", "yes"},
)

# リアルタイム状態管理用グローバル変数（スレッドセーフにするためのロック付き）
data_lock = threading.Lock()
current_force = 0
last_db_save_time = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
last_occupied_time = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

# ==========================================
# 🔌 ソケット通信処理 (ラズパイ受信 & 1時間ログ保存ルール)
# ==========================================
def save_hourly_vacancy_status(now_utc, force):
    """
    1時間に1回だけ呼ばれる。その時点のforce値から空席数を計算し、
    daily_vacancy_status に1行追加する（グラフ・管理者ログが参照する集計テーブル）。
    実機は1番席のみなので、利用中なら19席、空席なら20席として記録する。
    """
    now_jst = now_utc.astimezone(JST)
    target_week = f"{now_jst.month}/{now_jst.day}"
    day_of_week = WEEKDAY_JP[now_jst.weekday()]
    vacancy = 19 if force >= OCCUPIED_THRESHOLD else 20

    conn_db = None
    try:
        conn_db = get_db_connection()
        with conn_db.cursor() as cur:
            cur.execute(
                "INSERT INTO daily_vacancy_status (time, target_week, day_of_week, predicted_vacancy) VALUES (%s, %s, %s, %s)",
                (now_utc, target_week, day_of_week, vacancy)
            )
        conn_db.commit()
        print(f"✅ daily_vacancy_status保存完了: {now_utc} ({day_of_week} {vacancy}席)")
    except Exception as e:
        print("❌ daily_vacancy_status保存エラー:", e)
    finally:
        if conn_db:
            conn_db.close()


def handle_client(conn):
    global last_db_save_time, last_occupied_time, current_force
    try:
        data = conn.recv(1024)
        if not data:
            return

        raw_str = data.decode('utf-8').strip()
        print("📩 受信データ:", raw_str)
        d = json.loads(raw_str)
        force = float(d.get("force", 0))
        now = datetime.datetime.now(datetime.timezone.utc)

        with data_lock:
            # 1. リアルタイム状態の更新（毎回反映）
            current_force = force
            if force >= OCCUPIED_THRESHOLD:
                last_occupied_time = now  # 座った瞬間にタイマーリセット

            # 2. 生ログは受信の都度、毎回 daily_sensoe_log に貯める
            try:
                conn_db = get_db_connection()
                with conn_db.cursor() as cur:
                    cur.execute(
                        "INSERT INTO daily_sensoe_log (time, device_id, name, force) VALUES (%s, %s, %s, %s)",
                        (now, d.get("id"), "FSR406", str(force))
                    )
                conn_db.commit()
                conn_db.close()
                print(f"✅ daily_sensoe_log保存完了: {now}")
            except Exception as e:
                print("❌ daily_sensoe_log保存エラー:", e)

            # 3. daily_vacancy_status への集計保存は1時間に1回だけ
            if (now - last_db_save_time).total_seconds() >= 3600:
                save_hourly_vacancy_status(now, force)
                last_db_save_time = now

    except Exception as e:
        print("❌ 受信エラー:", e)
    finally:
        conn.close()


def run_socket_server():
    """ソケットサーバーを別スレッドで立ち上げる"""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((hostname, socket_port))
    server.listen(5)
    print(f"🚀 ソケットサーバー起動中: {hostname}:{socket_port}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


# ==========================================
# 🗄️ データベース接続・取得ロジック
# ==========================================
def create_tables_if_not_exists():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_sensoe_log (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                time timestamp with time zone NOT NULL,
                device_id text,
                name text,
                force text
            );
            """)
        conn.commit()
    finally:
        if conn:
            conn.close()


def fetch_next_week_prediction():
    """
    来週1週間分（月〜日）の空席予測を返す。

    【計算方法】単回帰（最小二乗法）によるトレンド予測。
    daily_vacancy_statusを曜日×対象日（target_week）ごとにグルーピングし、
    「その日1日分の平均空席数」を1つの時系列データ点とする。
    曜日ごとに直近WINDOW件の時系列点に対して y = a + b*x の直線を
    最小二乗法でフィットし、その延長線上（次の1点）の値を予測値とする。
    （b：傾き＝増加/減少のトレンド、a：切片）
    データ点が0件の曜日は20（満席なし）、1件のみの曜日はその値をそのまま用いる
    （回帰には最低2点必要なため）。
    """
    conn = None
    predictions = {day: None for day in WEEKDAY_JP}
    sample_counts = {day: 0 for day in WEEKDAY_JP}
    trend_used = {day: False for day in WEEKDAY_JP}
    WINDOW = 8  # 直近何回分の記録を回帰に使うか（古すぎる傾向を引きずらないための窓幅）

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT day_of_week, AVG(predicted_vacancy) AS avg_vacancy, MIN(time) AS first_time
                FROM daily_vacancy_status
                GROUP BY day_of_week, target_week
                ORDER BY day_of_week, first_time ASC;
            """)
            rows = cur.fetchall()

        # 曜日ごとに「1日1点」の時系列（古い→新しい順）を組み立てる
        series = {day: [] for day in WEEKDAY_JP}
        for day, avg_vacancy, _first_time in rows:
            if day in series and avg_vacancy is not None:
                series[day].append(float(avg_vacancy))

        for day in WEEKDAY_JP:
            points = series[day][-WINDOW:]
            n = len(points)
            sample_counts[day] = n

            if n == 0:
                predictions[day] = 20
            elif n == 1:
                predictions[day] = round(points[0])
            else:
                # 単回帰（最小二乗法）: x = 0,1,...,n-1（記録の古い順） を用いて次の1点(x=n)を外挿
                xs = list(range(n))
                x_mean = sum(xs) / n
                y_mean = sum(points) / n
                numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, points))
                denominator = sum((x - x_mean) ** 2 for x in xs)
                slope = numerator / denominator if denominator != 0 else 0
                intercept = y_mean - slope * x_mean
                predicted = intercept + slope * n
                # 座席数として意味を持つ範囲(0〜20)にクリップする
                predictions[day] = max(0, min(20, round(predicted)))
                trend_used[day] = True

    except Exception as e:
        print("❌ 来週予測データ取得エラー:", e)
        for day in WEEKDAY_JP:
            if predictions[day] is None:
                predictions[day] = 20
    finally:
        if conn:
            conn.close()

    return predictions, sample_counts, trend_used


def fetch_all_vacancy_records(search_query=None):
    """管理者用: daily_vacancy_status（集計テーブル）からそのまま実績レコードを取得"""
    conn = None
    records = []
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if search_query:
                like_q = f"%{search_query}%"
                cur.execute("""
                    SELECT
                        TO_CHAR(time AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') as full_time,
                        target_week,
                        day_of_week,
                        predicted_vacancy
                    FROM daily_vacancy_status
                    WHERE target_week ILIKE %s OR day_of_week ILIKE %s
                    ORDER BY time DESC
                    LIMIT 100;
                """, (like_q, like_q))
            else:
                cur.execute("""
                    SELECT
                        TO_CHAR(time AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') as full_time,
                        target_week,
                        day_of_week,
                        predicted_vacancy
                    FROM daily_vacancy_status
                    ORDER BY time DESC
                    LIMIT 100;
                """)
            for r in cur.fetchall():
                records.append({
                    "time": r[0],
                    "target_week": r[1],
                    "day_of_week": r[2],
                    "predicted_vacancy": r[3]
                })
    except Exception as e:
        print("❌ ログ取得エラー:", e)
    finally:
        if conn:
            conn.close()
    return records


def get_hourly_vacancy_for_date(target_date_str):
    """
    指定された日付(YYYY-MM-DD)の、8:00〜21:00の1時間ごとの空席度(最大20)を
    daily_vacancy_status テーブル（1時間に1回保存される集計値）から取得して返す。
    データが存在しない過去の時間は「20席すべて空席」として処理します。
    """
    now = datetime.datetime.now(JST)
    today_str = now.strftime('%Y-%m-%d')

    hours = list(range(8, 22))
    result = []

    db_data = {}
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    EXTRACT(HOUR FROM time AT TIME ZONE 'Asia/Tokyo') as hr,
                    predicted_vacancy
                FROM daily_vacancy_status
                WHERE
                    time AT TIME ZONE 'Asia/Tokyo' >= %s::timestamp
                    AND time AT TIME ZONE 'Asia/Tokyo' < (%s::timestamp + interval '1 day')
                ORDER BY time ASC;
            """, (target_date_str, target_date_str))

            rows = cur.fetchall()
            for r in rows:
                hr = int(r[0])
                try:
                    db_data[hr] = int(r[1])
                except (TypeError, ValueError):
                    pass

    except Exception as e:
        print("❌ 過去ログ取得エラー:", e)
    finally:
        if conn:
            conn.close()

    # 8:00から21:00の枠にデータを綺麗にマッピング
    for h in hours:
        label = f"{h:02d}:00"

        # 今日の日付を検索していて、現在時刻よりも先の「未来」のデータは表示しない
        if target_date_str == today_str and h > now.hour:
            result.append({"time": label, "vacancy": None})
        else:
            # daily_vacancy_status に記録が無い時間帯は「20席すべて空席」とみなす
            result.append({"time": label, "vacancy": db_data.get(h, 20)})

    return result


# ==========================================
# 🌐 Flask ルーティング
# ==========================================
@app.route('/')
def index():
    global current_force, last_occupied_time

    # 1. ステータス判定 (20分ルール)
    #    ・force >= OCCUPIED_THRESHOLD(500)         → occupied（オレンジ・利用中）
    #    ・しきい値未満だが、直近に着席してから20分未満 → maybe（青・まだ使っているかも）
    #    ・しきい値未満のまま20分(1200秒)経過          → vacant（緑・空席に自動復帰）
    #    ※ current_force はRaspi側が一定間隔で常時送信し続けることで最新に保たれる。
    #      （しきい値超えの瞬間しか送らない実装だと、離席が一切検知できず本ロジックが機能しない）
    now = datetime.datetime.now(datetime.timezone.utc)
    with data_lock:
        elapsed = (now - last_occupied_time).total_seconds()
        if current_force >= OCCUPIED_THRESHOLD:
            status = "occupied"
        elif elapsed < VACANT_GRACE_SECONDS:
            status = "maybe"
        else:
            status = "vacant"

        force_to_display = current_force

    # 2. データ準備
    predictions, prediction_counts, trend_used = fetch_next_week_prediction()
    today_str = datetime.datetime.now(JST).strftime('%Y-%m-%d')
    target_date = request.args.get('date', today_str).strip()
    hourly_data = get_hourly_vacancy_for_date(target_date)
    is_admin = session.get('is_admin', False)

    # 3. 座席マップ用データ（実機は1番のみ。2〜20番はFIXED_DUMMY_OCCUPIED_SEATSによる
    #    固定ダミーデータで、リロードしても内容は変わらない）
    # 🎨 座席1番は3状態（occupied=オレンジ / maybe=青（離席後20分以内）/ vacant=緑）を
    #    そのまま status として渡す。ダミー座席(2〜20番)は occupied/vacant の2状態のみ。
    seat_1_occupied = (force_to_display >= OCCUPIED_THRESHOLD)
    seats = [{"num": 1, "occupied": seat_1_occupied, "status": status, "is_real": True}]
    seats += [{
        "num": i,
        "occupied": (i in FIXED_DUMMY_OCCUPIED_SEATS),
        "status": "occupied" if (i in FIXED_DUMMY_OCCUPIED_SEATS) else "vacant",
        "is_real": False
    } for i in range(2, 21)]

    return render_template('index.html',
                           latest_sensor={"force": force_to_display, "status": status},
                           predictions=predictions,
                           prediction_counts=prediction_counts,
                           trend_used=trend_used,
                           weekday_order=WEEKDAY_JP,
                           seat_1_occupied=seat_1_occupied,
                           seats=seats,
                           hourly_data=json.dumps(hourly_data),
                           target_date=target_date,
                           is_admin=is_admin)


@app.route('/admin-login', methods=['POST'])
def admin_login():
    if not ADMIN_PASSWORD:
        return jsonify({"status": "error", "message": "ADMIN_PASSWORDが設定されていません。"}), 503
    if hmac.compare_digest(request.form.get('password', ''), ADMIN_PASSWORD):
        session['is_admin'] = True
        return jsonify({"status": "success", "message": "認証に成功しました。"})
    return jsonify({"status": "fail", "message": "パスワードが正しくありません。"})


@app.route('/admin-logout')
def admin_logout():
    session.pop('is_admin', None)
    return redirect(url_for('index'))


@app.route('/admin/records')
def admin_records():
    if not session.get('is_admin', False):
        return jsonify({"status": "error", "message": "認証されていないアクセスです"}), 403

    search_q = request.args.get('q', '').strip()
    records = fetch_all_vacancy_records(search_query=search_q)
    return jsonify(records)


if __name__ == '__main__':
    create_tables_if_not_exists()

    # センサー受信用のサーバーを別スレッドで起動
    threading.Thread(target=run_socket_server, daemon=True).start()

    # Flask Webサーバー起動
    app.run(host='0.0.0.0', port=5001, debug=False)
