# Описание проекта для загрузки витрин (витрина CREDIT)

- параметры запуска
  - sbt.version=1.6.2
  - sparkVersion = "3.3.2"
  - scalaVersion := "2.13.8"
  - java (использовалась при проверках corretto 1.8)
  - OS: KUbuntu 22.04.1
  - IDEA 2023.1
  - MergeStrategy (для сборки fat jar)

### Структура проекта (вне корневого проекта)

- [sources](sources) - источник csv файлов
- [table](table) 
    - [credit](table%2Fstg%2Fcredit) папка с результатом(витриной) в формате orc, партицированная по значениям год-месяц
    - [csv](table%2Fstg%2Fcsv) результат в формате csv

### Структура проекта (корневой проект) [datamarts](src%2Fmain%2Fscala%2Ft1%2Fdatamarts)
- [Main.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2FMain.scala) главный объект, служит для запуска расчета
- [Parameters.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2FParameters.scala) Парсинг входных параметров (библиотека scallop)
- [extract](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fextract)
  - [schemas](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fextract%2Fschemas) Структуры таблиц в формате scala
  - [Connect.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fextract%2FConnect.scala) трейт, инициализация Spark сессии
  - [Extract.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fextract%2FExtract.scala) Класс для считывания источников и применения к ним фильтров
  <br>(Использует [Function.scala] и метод extractTable, а так же параметры фильтрации load-date)
  - [Function.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fextract%2FFunction.scala) содержит метод extractTable для загрузки из csv
  - [Variables.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fextract%2FVariables.scala) Переменные проекта
- [load](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fload)
  - [Clear.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fload%2FClear.scala) Класс для очистки временных данных Spark
  - [Load.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fload%2FLoad.scala) Класс производит запись витрины на диск, с инкрементом. После загрузки производится повторное чтение записанных данных, фильтрация на неактуальные данные, запись во временную дирректорию, после перезапись основной витрины.
  Временная дирректория очищается с помощью класса [Clear.scala]
- [processes](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Fprocesses) Добавляет аттрибут __TRANZACTION_DATE__ для инкремента данных, после расчета витрины
- [transform](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Ftransform)   
  - [Variables.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Ftransform%2FVariables.scala) переменные используемые при расчете витрины
  - [Transform.scala](src%2Fmain%2Fscala%2Ft1%2Fdatamarts%2Ftransform%2FTransform.scala) Класс с объектом компаньоном для расчета витрины. 
  Запускает извлечение данных из класса [Extract.scala] методы расчета расположены в объекте, вызов методов в функции run() класса.
  
> Проект выполнил: Богданов Антон
