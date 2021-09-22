import 'dart:async';
import 'package:parser_middleware/global.dart';
import 'package:postgres/postgres.dart';
import 'dart:convert';

class Database {
  Future<dynamic> getListOfFilesForProcessing(Map body) async {
  // получаем список файлов для работы
    try {
      List<List<dynamic>> result = await connection.query(body['sql']).timeout(Duration(minutes: 5));
      if (result.isNotEmpty) {
        return result;
      } else {
        print('No XML Files for processing'); // TODO: add some return
        return [];
      }
    } on PostgreSQLException catch (e) {
      print(e);
    }
  }

  Future<dynamic> getCountForProcessing(Map body) async {
    // получаем количество файлов для работы 
    try {
      List<List<dynamic>> result = await connection.query(body['sql']).timeout(Duration(minutes: 5));
      if (result.isNotEmpty) {
        print('COUNT for processing: ${result[0][0]}');
        return result[0][0];
      } else {
        print('COUNT: No files for processing'); 
        return [];
      }
    } on PostgreSQLException catch (e) {
      print(e);
    }
  }

  // сюда прилетает UPDATE для xml_files
  Future<dynamic> commandFromParser(Map body) async {
    try {
      // {'sql': 'UPDATE TABLE WHERE...'}
      List<List<dynamic>> result = await connection_fast.query(body['sql']).timeout(Duration(seconds: 120));
      return result;
    } on PostgreSQLException catch (e) {
      writeLog('commandFromParser', e.message);
    } catch (e) {
      print('commandFromParser exception on e: ${e}');
      writeLog('commandFromParser', e.toString());
    }
  }

  Future<dynamic> sqlInsert(Map body) async {
    var isDataWasInserted = dbEnums.insertFailed; // т.к. идет несколько попыток
    try {
      // данные прилетают как запросы через точку-запятую
      // {'sql': 'INSERT INTO TABLE1; INSERT INTO TABLE2; INSERT INTO TABLE3;'}
      await connection_fast.transaction((ctx) async {
        for (var s in jsonDecode(body['sql'])) {
          await ctx.query(s);
        }
        isDataWasInserted = dbEnums.success;
        // print("INSERT SUCCESS\n");
      }).timeout(Duration(seconds: 120));
    } on PostgreSQLException catch (e) {
      // если не смогли вставить, напечатаем то что не смогли вставить
      for (var s in jsonDecode(body['sql'])) {
        print('sql insert: $s');
      }

      print('FIRST INSERT FAILED: ${e.message} ');
      connection_fast.cancelTransaction();
      try {
        print('There is some duplicates. Removing them');
        // значит в таблице есть дубликаты, попробем их удалить. Другие причины почему не смогло вставиться пока не рассматриваем
        // {'sql': 'DELETE FROM TABLE1 WHERE ... ; DELETE FROM TABLE2 WHERE ... ; DELETE FROM TABLE3 WHERE ... ;'}
        await connection_fast.transaction((ctx) async {
          for (var s in jsonDecode(body['sql-remove'])) {
            await ctx.query(s);
          }
          // ок рубликаты удалены
          print('Duplicates removed');
        }).timeout(Duration(seconds: 90));

        // новая попытка вставить
        try {
          print('Second Insert');
          // снова попытаемя вставить
          await connection_fast.transaction((ctx) async {
            for (var s in jsonDecode(body['sql'])) {
              await ctx.query(s);
            }
            // print("INSERT2 SUCCESS");
            isDataWasInserted = dbEnums.success;
          }).timeout(Duration(seconds: 90));
          print('Second Insert Success');
        } on TimeoutException catch (e) {
          print('second attempt timeout error: ${e.message}');
        } on PostgreSQLException catch (e) {
          
          // если снова не вставилось, то значит вообще жопа какая-то
          print('PostgreSQLException SECOND INSERT WAS FAILED ${e.message}');
          connection_fast.cancelTransaction();
          print('failed insert: ');
            for (var s in jsonDecode(body['sql'])) {
              print('sql insert: $s');
            }
          // выйжем 
          writeLog('SECOND INSERT WAS FAILED ', e.message); // нельзя продолжать т.к. попытка вставки провалилась
        }
      } on TimeoutException catch (e) {
        print('removing dups timeout error: ${e.message}');
      } on PostgreSQLException catch (e) {
        print('Removing duplicates was FAILED: ${e.message}');
      }
    } on Exception catch (e) {
      print('Some unknown exception with base class: $e');
    } 
    
    return isDataWasInserted;
    
  }
}
