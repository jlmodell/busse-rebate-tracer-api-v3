import io
import pandas as pd


def GET_BYTES(df: pd.DataFrame, filename: str) -> bytes:
    with io.BytesIO() as output:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False)

            if filename.endswith(".xlsm"):
                workbook = writer.book
                workbook.add_vba_project("./vbaProject.bin")

        data = output.getvalue()

    return data


def GET_HTML_WITH_JS_CSS(df: pd.DataFrame) -> str:
    html = df.to_html(index=False)

    html += """
<select name="select" id="select" onchange="filterTable()">
  <option value="">Select a column</option>
"""
    try:
        periods = list(df["period"].unique())
    except KeyError:
        print("***********************************************")
        print(KeyError)
        print("see s3_functions/savers.py for more information")
        print("***********************************************")

        periods = ["A", "B"]

    for period in periods:
        html += f"""
  <option value="{period}">{period}</option>
"""

    html += """
</select>
<style type="text/css">
    table {
        border-collapse: collapse;
        border: 1px solid black;
        background-color: #f2f2f2;
        -fs-table-paginate: paginate;
    }

    thead {
        display: table-header-group;
    }

    tr {
        height: 50px;
        vertical-align: top;
    }

    tr:nth-child(even) {
        background: #ccc;
    }
    tr:nth-child(odd) {
        background: #fff;
    }

    select {
        position: absolute;
        top: 0;
        right: 0;
    }
</style>

<script type="text/javascript">
  function filterTable() {
    const query = (q) => document.querySelectorAll(q);
    const filters = [...query("select")].map((e) => new RegExp(e.value, "i"));

    console.log(filters);

    query("tbody tr").forEach(
      (row) =>
        (row.style.display = filters.every((f, i) =>
          f.test(row.cells[0].textContent)
        )
          ? ""
          : "none")
    );
  }
</script>
"""
    return html
