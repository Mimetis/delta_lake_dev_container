import json
import tempfile
from pathlib import Path
from siemens.spark_util import get_spark
from siemens.transform import json_dir_to_delta


def test_json_to_parquet_roundtrip():
    spark = get_spark("siemens-test")
    with tempfile.TemporaryDirectory() as tmp:
        input_dir = Path(tmp, "json")
        output_dir = Path(tmp, "parquet")
        input_dir.mkdir()

        records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        Path(input_dir, "data.json").write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")

        df = json_dir_to_delta(spark, str(input_dir), str(output_dir))
        assert df.count() == 2
    spark.stop()
