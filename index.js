var sql = require("mssql");
const readExcel = require("read-excel-file/node");
const reader = require("xlsx");
const err_result = reader.readFile("D:/DOP/err_result.xlsx");

var config = {
  user: "bahriadmin",
  password: "Xuqo1030$%123&",
  server: "ba-prod-asw-ondemand.sql.azuresynapse.net",
  database: "corporate_reports_hub",
  options: {
    encrypt: true, // for azure
    trustServerCertificate: false // change to true for local dev / self-signed certs
  }
};
var bu_name = "corporate";
var data_source = "corsource";
var pen_tbl = [];
var tbl_name = "";
var err_tbl = [];

sql.connect(config, async function(err) {
  if (err) throw err;
  console.log("Connected!");

  var request = new sql.Request();

  readExcel("D:/DOP/table_list.xlsx").then(async data => {
    try {
      for (i in data) {
        tbl_name = data[i][1];
        console.log(data[i][1] + " external table is in progress");
        var script = await create_tbl_script(data[i]);
        // console.log(script);

        if (script != "-1") {
          var result = await request.query(script);
          console.log(data[i][1] + " external table is created...");
          // console.log(result);
        } else {
          console.log(data[i][1] + " create external table is failed..");
          pen_tbl.push(data[i][1]);
        }
      }

      await update_err_tbl();
      sql.close();
      console.log(pen_tbl);
      console.log("Connection is close...");
    } catch (e) {
      console.log(e);
      console.log(pen_tbl);
    }
  });
});

async function create_tbl_script(data) {
  try {
    var tbl_name =
      "Tbl" + data[1].charAt(0).toUpperCase() + data[1].slice(1).toLowerCase();

    var columns = "";
    if (bu_name.toLowerCase() == "corporate")
      columns = await get_corporate_columns(data[1]);
    else columns = await check_col_data_types(data[2]);

    if (columns == "-1") return "-1";

    var tbl_credentials =
      "with ( location = 'dbo/" +
      data[1].toLowerCase() +
      "/**', data_source = [" +
      data_source +
      "] , file_format = [SynapseDelimitedTextFormat] ); ";

    if (bu_name.toLowerCase() == "corporate")
      tbl_credentials = tbl_credentials.replace(
        "dbo",
        data[1].split("_")[0].toLowerCase()
      );

    var query = "";
    query = "create external table [dbo]." + "[" + tbl_name + "]";
    query = query + " (" + columns + " ) ";
    query = query + tbl_credentials;
    query = query + "select '" + tbl_name + " table is created...' as res;";

    query =
      "if not EXISTS (select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME = '" +
      tbl_name +
      "') begin " +
      query +
      " end; else begin select 'Table is already is created' as res end;";

    return query;
  } catch (e) {
    console.log(e);
  }
}

function check_col_data_types(columns) {
  try {
    var arr = [];
    if (columns) {
      for (let item of columns.split(",")) {
        item = item.trim();

        if (item.split(" ")[1].includes("text")) {
          item = item.split(" ")[0] + " varchar(max)";
        } else if (item.split(" ")[1].includes("image")) {
          item = item.split(" ")[0] + " varbinary(max)";
        } else if (item.split(" ")[1].includes("-1")) {
          item = item.split(" ")[0] + " varchar(max)";
        } else if (item.split(" ")[1].includes("money")) {
          item = item.split(" ")[0] + " decimal";
        } else if (item.split(" ")[1].includes("real")) {
          item = item.split(" ")[0] + " float";
        }

        arr.push(item);
      }
    }

    return arr.toString();
  } catch (e) {
    console.log(e);
  }
}

async function get_corporate_columns(tbl_name) {
  try {
    var openrowset_query = `
      SELECT
        TOP 1 *
      FROM
          OPENROWSET(
              BULK @BULK,
              FORMAT = 'CSV',
              PARSER_VERSION = '2.0',
          DATA_SOURCE = @DATA_SOURCE
          ) AS [result]`;

    var bulk =
      tbl_name.split("_")[0].toLowerCase() +
      "/" +
      tbl_name.toLowerCase() +
      "/**";
    openrowset_query = openrowset_query.replace("@BULK", "'" + bulk + "'");
    openrowset_query = openrowset_query.replace(
      "@DATA_SOURCE",
      "'" + data_source + "'"
    );
    openrowset_query = openrowset_query.trim();

    var pool = await sql.connect(config);
    var result = await pool.request().query(openrowset_query);

    var column = [];

    if (result.recordset)
      for (let item of Object.keys(result["recordset"][0])) {
        column.push(
          String(result["recordset"][0][item]) + " " + "varchar(500)"
        );
      }

    return column.toString();
  } catch (e) {
    console.log(String(e));
    err_tbl.push({
      "Table Name": tbl_name,
      "Error Message": String(e)
    });
    return "-1";
  }
}

async function update_err_tbl() {
  try {
    const d = new Date().toLocaleString("en-US", {
      timeZone: "Asia/Kolkata",
      hour12: false
    });

    const date_time =
      d.split(",")[0].split("/").reverse().join("") +
      "_" +
      d.split(",")[1].split(":").join("");

    const ws = reader.utils.json_to_sheet(err_tbl);
    await reader.utils.book_append_sheet(
      err_result,
      ws,
      bu_name + "_" + date_time
    );
    await reader.writeFile(err_result, "D:/DOP/err_result.xlsx");
    return "1";
  } catch (e) {
    console.log(e);
  }
}

// ****sql****
// Select T.TABLE_SCHEMA, T.TABLE_NAME
//     , Stuff(
//         (
//         Select ', '+ '[' + C.COLUMN_NAME+ '] '+ c.DATA_TYPE +(case when c.CHARACTER_MAXIMUM_LENGTH is null then '' else '('+ convert(varchar, c.CHARACTER_MAXIMUM_LENGTH)+')' end)
//         From INFORMATION_SCHEMA.COLUMNS As C
//         Where C.TABLE_SCHEMA = T.TABLE_SCHEMA
//             And C.TABLE_NAME = T.TABLE_NAME
//         Order By C.ORDINAL_POSITION
//         For Xml Path('')
//         ), 1, 2, '') As Columns
// From INFORMATION_SCHEMA.TABLES As T
//     Left Join INFORMATION_SCHEMA.VIEWS As V
//         On V.TABLE_SCHEMA = T.TABLE_SCHEMA
//             And V.TABLE_NAME = T.TABLE_NAME
// Where V.TABLE_NAME Is Null and t.TABLE_NAME in ()

// ======================================================

// ****oracle****
// select  table_name, LISTAGG('[' || column_name || '] ' || data_type || '(' || data_length || ')',',') within group (order by column_id)
// FROM ALL_TAB_COLS
// group by table_name

// select [db_name],''''+UPPER(REPLACE(table_name,'Tbl',''))+''',' as tbl_name,table_name from liner_migration_order
// where is_active = '1'
// and table_name not in ()
