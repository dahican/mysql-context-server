#!/usr/bin/env node

import mysql from "mysql2/promise";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListPromptsRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
  GetPromptRequestSchema,
  CompleteRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

const server = new Server({
  name: "mysql-context-server",
  version: "0.1.0",
});

const databaseUrl = process.env.DATABASE_URL;
if (typeof databaseUrl == null || databaseUrl.trim().length === 0) {
  console.error("Please provide a DATABASE_URL environment variable");
  process.exit(1);
}

const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "mysql:";
resourceBaseUrl.password = "";

process.stderr.write("starting server. url: " + databaseUrl + "\n");
// Parse the MySQL connection URI and create a pool
const connectionUrl = new URL(databaseUrl);
const pool = mysql.createPool({
  host: connectionUrl.hostname,
  port: connectionUrl.port || 3306,
  user: connectionUrl.username,
  password: connectionUrl.password,
  database: connectionUrl.pathname.substring(1), // remove leading slash
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

const SCHEMA_PATH = "schema";
const SCHEMA_PROMPT_NAME = "mysql-schema";
const ALL_TABLES = "all-tables";

server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const connection = await pool.getConnection();
  try {
    const [rows] = await connection.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()",
    );
    return {
      resources: rows.map((row) => ({
        uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href,
        mimeType: "application/json",
        name: `"${row.table_name}" database schema`,
      })),
    };
  } finally {
    connection.release();
  }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);

  const pathComponents = resourceUrl.pathname.split("/");
  const schema = pathComponents.pop();
  const tableName = pathComponents.pop();

  if (schema !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }

  const connection = await pool.getConnection();
  try {
    const [rows] = await connection.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?",
      [tableName],
    );

    return {
      contents: [
        {
          uri: request.params.uri,
          mimeType: "application/json",
          text: JSON.stringify(rows, null, 2),
        },
      ],
    };
  } finally {
    connection.release();
  }
});

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "mysql-schema",
        description: "Returns the schema for a MySQL database.",
        inputSchema: {
          type: "object",
          properties: {
            mode: {
              type: "string",
              enum: ["all", "specific"],
              description: "Mode of schema retrieval",
            },
            tableName: {
              type: "string",
              description:
                "Name of the specific table (required if mode is 'specific')",
            },
          },
          required: ["mode"],
          if: {
            properties: { mode: { const: "specific" } },
          },
          then: {
            required: ["tableName"],
          },
        },
      },
      {
        name: "query",
        description: "Run a read-only SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name === "mysql-schema") {
    const mode = request.params.arguments?.mode;

    const tableName = (() => {
      switch (mode) {
        case "specific": {
          const tableName = request.params.arguments?.tableName;

          if (typeof tableName !== "string" || tableName.length === 0) {
            throw new Error(`Invalid tableName: ${tableName}`);
          }

          return tableName;
        }
        case "all": {
          return ALL_TABLES;
        }
        default:
          throw new Error(`Invalid mode: ${mode}`);
      }
    })();

    const connection = await pool.getConnection();

    try {
      const sql = await getSchema(connection, tableName);

      return {
        content: [{ type: "text", text: sql }],
      };
    } finally {
      connection.release();
    }
  }

  if (request.params.name === "query") {
    const sql = request.params.arguments?.sql;

    const connection = await pool.getConnection();
    try {
      // Start a read-only transaction in MySQL
      await connection.query("SET SESSION TRANSACTION READ ONLY");
      await connection.query("START TRANSACTION");
      const [rows] = await connection.query(sql);
      return {
        content: [
          { type: "text", text: JSON.stringify(rows, undefined, 2) },
        ],
      };
    } catch (error) {
      throw error;
    } finally {
      connection.query("ROLLBACK")
        .catch((error) =>
          console.warn("Could not roll back transaction:", error),
        );

      connection.release();
    }
  }

  throw new Error("Tool not found");
});

server.setRequestHandler(CompleteRequestSchema, async (request) => {
  process.stderr.write("Handling completions/complete request\n");

  if (request.params.ref.name === SCHEMA_PROMPT_NAME) {
    const tableNameQuery = request.params.argument.value;
    const alreadyHasArg = /\S*\s/.test(tableNameQuery);

    if (alreadyHasArg) {
      return {
        completion: {
          values: [],
        },
      };
    }

    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()",
      );
      const tables = rows.map((row) => row.table_name);
      return {
        completion: {
          values: [ALL_TABLES, ...tables],
        },
      };
    } finally {
      connection.release();
    }
  }

  throw new Error("unknown prompt");
});

server.setRequestHandler(ListPromptsRequestSchema, async () => {
  process.stderr.write("Handling prompts/list request\n");

  return {
    prompts: [
      {
        name: SCHEMA_PROMPT_NAME,
        description:
          "Retrieve the schema for a given table in the mysql database",
        arguments: [
          {
            name: "tableName",
            description: "the table to describe",
            required: true,
          },
        ],
      },
    ],
  };
});

server.setRequestHandler(GetPromptRequestSchema, async (request) => {
  process.stderr.write("Handling prompts/get request\n");

  if (request.params.name === SCHEMA_PROMPT_NAME) {
    const tableName = request.params.arguments?.tableName;

    if (typeof tableName !== "string" || tableName.length === 0) {
      throw new Error(`Invalid tableName: ${tableName}`);
    }

    const connection = await pool.getConnection();

    try {
      const sql = await getSchema(connection, tableName);

      return {
        description:
          tableName === ALL_TABLES
            ? "all table schemas"
            : `${tableName} schema`,
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: sql,
            },
          },
        ],
      };
    } finally {
      connection.release();
    }
  }

  throw new Error(`Prompt '${request.params.name}' not implemented`);
});

/**
 * @param tableNameOrAll {string}
 */
async function getSchema(connection, tableNameOrAll) {
  const select =
    "SELECT column_name, data_type, is_nullable, column_default, table_name FROM information_schema.columns";

  let rows;
  if (tableNameOrAll === ALL_TABLES) {
    [rows] = await connection.query(
      `${select} WHERE table_schema = DATABASE()`,
    );
  } else {
    [rows] = await connection.query(`${select} WHERE table_name = ?`, [
      tableNameOrAll,
    ]);
  }

  const allTableNames = Array.from(
    new Set(rows.map((row) => row.table_name).sort()),
  );

  let sql = "```sql\n";
  for (let i = 0, len = allTableNames.length; i < len; i++) {
    const tableName = allTableNames[i];
    if (i > 0) {
      sql += "\n";
    }

    sql += [
      `CREATE TABLE \`${tableName}\` (`,
      rows
        .filter((row) => row.table_name === tableName)
        .map((row) => {
          const notNull = row.is_nullable === "NO" ? " NOT NULL" : "";
          const defaultValue =
            row.column_default != null ? ` DEFAULT ${row.column_default}` : "";
          return `    \`${row.column_name}\` ${row.data_type}${notNull}${defaultValue}`;
        })
        .join(",\n"),
      ");",
    ].join("\n");
    sql += "\n";
  }
  sql += "```";

  return sql;
}

async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

runServer().catch((error) => {
  console.error(error);
  process.exit(1);
});
