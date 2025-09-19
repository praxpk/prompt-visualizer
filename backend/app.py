from flask import Flask, jsonify
import duckdb


def create_app() -> Flask:
    app = Flask(__name__)

    @app.route("/health", methods=["GET"])
    def health():
        try:
            # Simple DB check to ensure DuckDB can execute
            res = duckdb.execute("SELECT 1").fetchone()
            db_ok = bool(res and res[0] == 1)
        except Exception as e:
            return jsonify({"status": "error", "db": False, "error": str(e)}), 500

        return jsonify({"status": "ok", "db": db_ok}), 200

    return app


if __name__ == "__main__":
    app = create_app()
    # Bind to all interfaces for local testing / docker use
    app.run(host="0.0.0.0", port=5000, debug=True)

