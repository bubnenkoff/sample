import 'dart:io';
import 'package:parser_middleware/database.dart';
import 'package:postgres/postgres.dart';

enum dbEnums { success, alreadyExists, insertFailed, purchaseDoNotExits }

late PostgreSQLConnection connection;
late PostgreSQLConnection connection_fast;
late Database db;

String logFile = 'error.log';

writeLog(String request, String? error) {

  var errorString = '$request \n$error \n--\n';

  print(errorString);
  File(logFile).writeAsStringSync(errorString, mode: FileMode.append);
  File('pid.txt').delete();
  exit(1);
}

