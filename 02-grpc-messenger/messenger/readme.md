Лог успешного прогона (10 баллов) положил в proofs/grpc\_messenger\_cli\_test.txt , скрин с консоли также приложил, image.png



......Мессенджер на gRPC

....Питч идеи:

..Простой общий чат с gRPC сервером и двумя типами запросов:

SendMessage (unary) и ReadMessages (server stream)

..Сервер не хранит историю, т.е. если клиент подключился позже, старые сообщения не получит - по условию

..Клиент поднимает HTTP (POST /sendMessage и POST /getAndFlushMessages) и параллельно в фоне держит gRPC стрим, складывает всё в локальный буфер

..По порядку: каждый клиент должен видеть сообщения в том же порядке, как они поступили на сервер. Для этого на сервере делаю одну общую крит секцию, и на каждого подписчика отдельную очередь FIFO.

..Сервер многопоточный через grpc server + ThreadPoolExecutor, общие структуры защищены локом

..sendTime делаю через google.protobuf.Timestamp, с гарантей что время строго возрастает, по условию тестов



....Структура файлов 

..messenger/proto/messenger.proto - описание сообщений и сервиса MessengerServer (SendMessage, ReadMessages)

..messenger/server/server.py - gRPC сервер, хранит список подписчиков (очередей) и рассылает им сообщения

..messenger/client/client.py - http сервер+grpc клиент, поток чтения стрима и буферизация

..messenger/server.dockerfile - сборка образа сервера

..messenger/client.dockerfile - сборка образа клиента

..messenger/server/requirements.txt , messenger/client/requirements.txt - зависимости python (grpcio, grpcio-tools)

..messenger/init.py и подпакеты init.py - чтобы импорты не отваливались





......gRPC интерфейс (proto)

....Идея:

..По условию сервис обязан называться MessengerServer, и поля строго author,text,sendTime

..sendTime тип google.protobuf.Timestamp



....Сообщения:

..SendMessageRequest{author,text}

..SendMessageResponse{send\_time}

..ChatMessage{author,text,send\_time}

..ReadMessages принимает google.protobuf.Empty и возвращает stream ChatMessage





......Сервер

....Идея:

..Сервер не кэширует на будущее, а только пушит новые сообщения во все активные стримы

..Каждый стрим ReadMessages это отдельная подписка со своей очередью, чтобы не блокировать других подписчиков и сохранять порядок на каждого клиента



....Состояние сервера:

..self.\_subs - список очередей, по одной на каждый активный ReadMessages

..self.\_lock - чтобы одновременно SendMessage и ReadMessages не ломая список

..self.\_last\_ns - последний выданный timestamp в ns (чтоб время было строго возрастающим)



....SendMessage(author,text) в sendTime

..беру lock

..генерю timestamp через time.time\_ns(), но если вдруг ns не увеличился (очень быстрые вызовы), делаю ns = last+1

..собираю ChatMessage(author,text,send\_time)

..копию ChatMessage кладу в очередь каждого подписчика (fifo)

..возвращаю SendMessageResponse(send\_time)



....ReadMessages(Empty) в stream ChatMessage

..создаю новую queue.Queue()

..под lock добавляю её в self.\_subs

..дальше в цикле: q.get(timeout=1) и yield сообщения

..если клиент отвалился / context не эктив, то выхожу и в finally удаляю очередь из self.\_subs



Порядок сохраняется, тк SendMessage оборачивается в одну крит секцию, значит сообщения попадают в очереди подписчиков ровно в одном порядке; queue.Queue FIFO, значит каждый подписчик читает сообщения в таком же порядке



....Почему сервер потокобезопасен

..self.\_subs меняется только под lock

..timestamp last\_ns тоже только под lock

..очереди потокобезопасны сами по себе (queue.Queue)





......Клиент

..Клиент состоит из 2 потоков:



основной: хттп серве, однопоточный как в заготовке)

фоновый - gRPC consumer (читает ReadMessages и складывает в буфер)

..Общий буфер сделан как postbox с локом внутри, чтобы http поток и consumer не мешали друг другу



....PostBox

..self.\_messages: List\[Dict]

..self.\_lock: threading.Lock

..put\_message кладёт в список под lock

..collect\_messages: deepcopy списка и очистка, тоже под lock 



....HTTP API

..POST /sendMessage:

парсю json

делаю protobuf SendMessageRequest через ParseDict

stub.SendMessage(req)

возвращаю {"sendTime": "..."} 

..POST /getAndFlushMessages:

возвращаю список сообщений из postbox.collect\_messages()



....gRPC stream consumer

..в фоне запускаю thread, который делает stub.ReadMessages(Empty()) и в цикле for msg in call:

складываю {"author":..., "text":..., "sendTime":...} в постбокс

..если стрим упал (grpc.RpcError) делаю небольшой sleep и переподключаюсь



....Отдельно выделю:

..По условию сервер не шлёт историю, так что я стараюсь поднять gRPC соединение и стартануть ReadMessages до того, как HTTP начнёт реально принимать запросы. те сначала запуск консюмера, небольшой вэйт, и только потом serve\_forever





....Формат sendTime

..Timestamp конверчу в строку вида 2025-..T..:..:.. .123456789Z

таким образом nanos всегда 9 цифр, и сортировка совпадает по времени и тесты нормально сравниваются





......Docker образы

....server.dockerfile

..ставлю зависимости

..копирую messenger/ внутрь

..компилю proto через grpc\_tools.protoc прямо при сборке образа, чтобы всегда были messenger\_pb2.py и messenger\_pb2\_grpc.py

..entrypoint запускает python -m messenger.server.server



....client.dockerfile

..аналогично: зависимости + копия messenger + компиляция proto

..entrypoint запускает python -m messenger.client.client



....Переменные окружения

..сервер слушает 0.0.0.0:$MESSENGER\_SERVER\_PORT

..клиент подключается к $MESSENGER\_SERVER\_ADDR

..клиент поднимает http на 0.0.0.0:$MESSENGER\_HTTP\_PORT





......Итог

..proto описан и совпадает по требованиям

..сервер многопоточный и потокобезопасный, рассылает только новые сообщения активным стримам

..клиент читает stream в фоне, буферизует, и отдаёт буфер через /getAndFlushMessages

