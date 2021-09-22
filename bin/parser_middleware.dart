import 'dart:async';
import 'package:parser_middleware/database.dart';
import 'package:alfred/alfred.dart';
import 'package:postgres/postgres.dart';
import 'package:parser_middleware/global.dart';
import 'package:alfred/src/middleware/cors.dart';

const int PORT = 5000;

void main() async {

  db = Database();

  // два коннекшена с разными именами т.к. такое чувство то если на один пааеттяжелый запрос, то второй блочится хз почему
  // разумеется должен остаться один коннекшен, это я и пытаюсь узнать. Может драйвер течет или я хз
  connection = PostgreSQLConnection('localhost', 5432, 'fz44', username: 'postgres', password: '123', queryTimeoutInSeconds: 600);
  connection_fast = PostgreSQLConnection('localhost', 5432, 'fz44', username: 'postgres', password: '123', queryTimeoutInSeconds: 90);
  await connection.open();
  await connection_fast.open();

  final app = Alfred();
  app.all('*', cors(origin: '*', headers: '*'));

  // Post Request only for healt-check
  app.post('/', (req, res) async {
    await res.json({});
  });

  app.post('/parsing', (req, res) async {
    // print('/parsing handler');
    final body = await req.body; //JSON body
    var files = await db.getListOfFilesForProcessing(body as Map<String, dynamic>);
    await res.json({'data': files});
  });

  // обычно сюда только update прилетают для xml_files
  app.post('/commands', (req, res) async {
    // print('commands handler');
    final body = await req.body; //JSON body
    print(body);
    try {
      var r = await db
          .commandFromParser(body as Map<String, dynamic>)
          .timeout(Duration(seconds: 120));
      await res.json({'status': 'success', 'data': r});
    } catch (e) {
      print('[ERROR] in commands handle: $e');
    }
  });

  app.post('/count', (req, res) async {
    // print('count handler');
    final body = await req.body; //JSON body
    try {
      var r = await db.getCountForProcessing(body as Map<String, dynamic>).timeout(Duration(minutes: 2));
      await res.json({'status': 'success', 'data': r});
    } catch (e) {
      await res.json({'status': 'success', 'data': 0});
      print('[ERROR] count handler: $e');
    }
  });

  app.post('/sql-insert', (req, res) async {
    // print('sql-insert handler');
    final body = await req.body; //JSON body
    try {
      var insertResult = await db.sqlInsert(body as Map<String, dynamic>).timeout(Duration(seconds: 140));

      switch (insertResult) {
        // только в этих двух случаях есть смысл продолжать
        // в остальных нужно чтобы была устранена причина ошибки
        case dbEnums.success:
          await res.json({'status': 'success'});
          break;
        case dbEnums.insertFailed:
          await res.json({'status': 'failed'});
          break;
        default:
          await res.json({'status': 'unkownStatus'});
          break;
      }
    } on TimeoutException catch (e) {
      print('Too long request: ${e}');
    } catch (e) {
      print('[ERROR] sql-insert exception: $e');
    }
  });

  await app.listen(PORT);
}
