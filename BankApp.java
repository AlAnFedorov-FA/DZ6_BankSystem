import java.util.concurrent.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.ScheduledThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;

import java.util.*;
//import java.util.ArrayList;
//import java.util.List;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import java.io.InputStream;
import java.util.Properties;

import java.io.IOException;

// Класс Клиентов
class Client {
    // Уникальный идентификатор клиента
    private final int id;

    // Баланс клиента
    private double balance;

    // Валюта счета
    private String currency;

    // Имя клиента
    private String name;

    // Дата создания аккаунта
    private final long creationDate;

    // Конструктор
    public Client(int id, double initialBalance, String currency, String name) {
        this.id = id;
        this.balance = initialBalance;
        this.currency = currency;
        this.name = name;
        this.creationDate = System.currentTimeMillis();
    }

    // Получить ID клиента
    public int getId() {
        return id;
    }

    // Получить баланс
    public synchronized double getBalance() {
        return balance;
    }

    // Установить новый баланс
    public synchronized void setBalance(double balance) {
        this.balance = balance;
    }

    // Пополнить счет
    public synchronized boolean deposit(double amount) {
        if (amount > 0) {
            balance += amount;
            return true;
        }
        return false;
    }

    // Снять средства, проверка баланса
    public synchronized boolean withdraw(double amount) {
        if (amount > 0 && amount <= balance) {
            balance -= amount;
            return true;
        }
        return false;
    }

    // Получить валюту счета
    public String getCurrency() {
        return currency;
    }

    // Установить новую валюту
    public void setCurrency(String currency) {
        this.currency = currency;
    }

    // Получить имя клиента
    public String getName() {
        return name;
    }

    // Установить новое имя
    public void setName(String name) {
        this.name = name;
    }

    // Получить дату создания
    public long getCreationDate() {
        return creationDate;
    }

    @Override
    public String toString() {
        return String.format(
                "Клиент №%d: %s (%s), баланс: %.2f %s",
                id, name, new java.text.SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(creationDate),
                balance, currency
        );
    }
}

// Кассы, исполнители транзакций
class Cashier implements Runnable {
    // Идентификатор кассы
    private final int id;

    // Ссылка на банк
    private final Bank bank;

    // Флаг работы
    private volatile boolean running = true;

    // Время обработки транзакции
    private long transactionProcessingTime;

    // Конструктор
    public Cashier(int id, Bank bank) {
        this.id = id;
        this.bank = bank;
    }

    // Основной метод выполнения
    @Override
    public void run() {
        try {
            while (running) {
                // Получаем транзакцию из очереди
                Transaction transaction = bank.getTransactionQueue().take();

                // Обрабатываем транзакцию
                processTransaction(transaction);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            System.out.println("Касса " + id + " завершает работу");
        }
    }

    // Обработка транзакции
    private void processTransaction(Transaction transaction) {
        try {
            long startTime = System.currentTimeMillis();

            if (transaction == null) {
                bank.notifyObservers("Касса " + id + ": получена пустая транзакция");
                return;
            }

            try {
                transaction.execute(bank, id);
                bank.incrementTransactionsCount(); // УВЕЛИЧЕНИЕ СЧЕТЧИКА
            } catch (Exception e) {
                bank.notifyObservers("Касса " + id + ": ошибка при обработке транзакции " +
                        transaction.getType() + ": " + e.getMessage());
                return;
            }

            long processingTime = System.currentTimeMillis() - startTime; // время выполнения
            bank.notifyObservers("Касса " + id + ": транзакция " + transaction.getType() +
                    " обработана за " + processingTime + " мс");
        } catch (Exception e) {
            bank.notifyObservers("Критическая ошибка обработки на кассе " + id + ": " + e.getMessage());
        } finally {
            if (!running) {
                bank.notifyObservers("Касса " + id + " завершает работу");
            }
        }
    }


    // Остановка работы кассы
    public void shutdown() {
        running = false;
    }

    // Получение ID кассы
    public int getId() {
        return id;
    }

    // Получение времени последней обработки
    public long getTransactionProcessingTime() {
        return transactionProcessingTime;
    }
}

// Абстрактный класс для всех транзакций
abstract class Transaction {
    protected final int clientId;
    protected final TransactionType type;

    public Transaction(int clientId, TransactionType type) {
        this.clientId = clientId;
        this.type = type;
    }

    public int getClientId() {
        return clientId;
    }

    public TransactionType getType() {
        return type;
    }

    public abstract void execute(Bank bank, int cashierId);
}

// Перечисление типов транзакций
enum TransactionType {
    DEPOSIT,    // Пополнение
    WITHDRAW,   // Снятие
    EXCHANGE,   // Обмен валюты
    TRANSFER    // Перевод
}


// Интерфейс наблюдателя
interface Observer {
    void update(String message);
}

class Bank {
    // Хранилище клиентов
    private final  ConcurrentHashMap<Integer, Client> clients = new ConcurrentHashMap<>();

    // Курсы валют
    private final ConcurrentHashMap<String, Double> exchangeRates = new ConcurrentHashMap<>();

    // Очередь транзакций
    private final LinkedBlockingQueue<Transaction> transactionQueue = new LinkedBlockingQueue<>();

    // Список наблюдателей
    private final List<Observer> observers = new ArrayList<>();

    // Сервис для управления кассами
    private final ExecutorService executorService;

    // Список касс
    private final List<Cashier> cashiers = new ArrayList<>();

    // Счетчик транзакций обработанных
    private AtomicInteger processedTransactionsCount = new AtomicInteger(0);

    // Счетчик транзакций поданных
    private AtomicInteger submittedTransactionsCount = new AtomicInteger(0);

    public void incrementTransactionsCount() {
        processedTransactionsCount.incrementAndGet();
    }
    public int getProcessedTransactionsCount() {
        return processedTransactionsCount.get();
    }

    public int getSubmittedTransactionsCount() {
        return submittedTransactionsCount.get();
    }


    // Конструктор с указанием количества касс
    public Bank(int numberOfCashiers) {
        executorService = Executors.newFixedThreadPool(numberOfCashiers);

        // Создание касс
        for (int i = 1; i <= numberOfCashiers; i++) {
            Cashier cashier = new Cashier(i, this);
            cashiers.add(cashier);
        }

        // Запуск касс через ExecutorService
        startCashiers();

        // Дефолтные курсы валют
        exchangeRates.put("USD", 1.0);
        exchangeRates.put("EUR", 0.86);
        exchangeRates.put("RUB", 80.97);
    }


    // Метод запуска касс
    public void start() {
        if (!executorService.isShutdown()) {
            startCashiers();
            notifyObservers("Кассы запущены");
        } else {
            notifyObservers("Невозможно запустить: сервис уже остановлен");
        }
    }
// Каждая касса попадает в пул потоков
    private void startCashiers() {
        for (Cashier cashier : cashiers) {
            executorService.submit(cashier);
        }
    }

    // Добавление наблюдателя
    public void addObserver(Observer observer) {
        observers.add(observer);
    }

    // Метод уведомления наблюдателей
    public synchronized void notifyObservers(String message) {
        synchronized (observers) {
            for (Observer observer : observers) {
                observer.update(message); // каждый наблюдатель получает уведомление (сейчас он 1)
            }
        }
    }

    // Методы работы с клиентами
    public void addClient(Client client) {
        clients.put(client.getId(), client);
    }

    public Client getClient(int id) {
        return clients.get(id);
    }

    // Методы работы с валютами
    public void setExchangeRate(String currency, double rate) {
        synchronized (exchangeRates) {
            if (rate <= 0) {
                throw new IllegalArgumentException("Курс валюты должен быть положительным");
            }
            exchangeRates.put(currency, rate);
            notifyObservers("Установлен новый курс для " + currency + ": " + rate);
        }
    }
// Метод конвертации: amount - сумма для конвертации, fromCurrency - исходная валюта, oCurrency - целевая валюта
    public double convertCurrency(double amount, String fromCurrency, String toCurrency) {
        synchronized (exchangeRates) {
            double fromRate = exchangeRates.get(fromCurrency);
            double toRate = exchangeRates.get(toCurrency);
            return amount * (toRate / fromRate);
        }
    }

    // Методы обработки транзакций
    // ПОПОЛНЕНИЕ
    public void deposit(int cashierId, int clientId, double amount) {
        if (amount <= 0) {
            notifyObservers("Ошибка: попытка пополнения на сумму " + amount +
                    " для клиента " + clientId);
            return;
        }

        Client client = clients.get(clientId);
        if (client == null) {
            notifyObservers("Ошибка: клиент с ID " + clientId + " не найден");
            return;
        }

        try {
            synchronized (client) {
                client.deposit(amount);
            }

            notifyObservers("Касса " + cashierId +
                    ": операция пополнения клиента " + clientId +
                    ", сумма: " + amount +
                    ", новый баланс: " + client.getBalance());

        } catch (Exception e) {
            notifyObservers("Критическая ошибка при пополнении счета клиента " +
                    clientId + ": " + e.getMessage());
        }
    }

// СНЯТИЕ
public void withdraw(int cashierId, int clientId, double amount) {
    // Проверка корректности суммы
    if (amount <= 0) {
        notifyObservers("Ошибка: попытка снятия отрицательной суммы " + amount +
                " для клиента " + clientId);
        return;
    }

    Client client = clients.get(clientId);

    // Проверка существования клиента
    if (client == null) {
        notifyObservers("Ошибка: клиент с ID " + clientId + " не найден");
        return;
    }

    try {
        synchronized (client) {
            // Проверка достаточности средств
            if (client.getBalance() >= amount) {
                if (!client.withdraw(amount)) {
                    throw new Exception("Не удалось выполнить снятие средств");
                }

                // Логирование успешной операции
                notifyObservers("Касса " + cashierId +
                        ": успешное снятие средств клиентом " + clientId +
                        ", сумма: " + amount +
                        ", остаток: " + client.getBalance());
            } else {
                // Обработка недостатка средств
                notifyObservers("Ошибка: недостаточно средств у клиента " + clientId +
                        ", баланс: " + client.getBalance() +
                        ", запрошено: " + amount);
            }
        }
    } catch (Exception e) {
        // Обработка критических ошибок
        notifyObservers("Критическая ошибка при снятии средств клиентом " +
                clientId + ": " + e.getMessage());
    }
}

// ПЕРЕВОД
public void transferFunds(int cashierId, int senderId, int receiverId, double amount) {
    // Проверка корректности суммы
    if (amount <= 0) {
        notifyObservers("Ошибка: попытка перевода отрицательной суммы " + amount);
        return;
    }

    Client sender = clients.get(senderId);
    Client receiver = clients.get(receiverId);

    // Проверка существования клиентов
    if (sender == null) {
        notifyObservers("Ошибка: отправитель с ID " + senderId + " не найден");
        return;
    }

    if (receiver == null) {
        notifyObservers("Ошибка: получатель с ID " + receiverId + " не найден");
        return;
    }

    // Определяем порядок блокировок для предотвращения deadlock'ов
    int firstId = Math.min(senderId, receiverId);
    int secondId = Math.max(senderId, receiverId);

    try {
        synchronized (clients.get(firstId)) {
            synchronized (clients.get(secondId)) {
                // Проверка достаточности средств
                if (sender.getBalance() >= amount) {
                    if (!sender.withdraw(amount)) {
                        throw new Exception("Не удалось выполнить снятие средств у отправителя");
                    }
                    if (!receiver.deposit(amount)) {
                        throw new Exception("Не удалось выполнить зачисление средств получателю");
                    }

                    // Логирование успешной операции
                    notifyObservers("Касса " + cashierId +
                            ": успешный перевод от клиента " + senderId +
                            " клиенту " + receiverId +
                            ", сумма: " + amount);
                } else {
                    // Обработка недостатка средств
                    notifyObservers("Ошибка: недостаточно средств у отправителя " + senderId +
                            ", баланс: " + sender.getBalance() +
                            ", запрошено: " + amount);
                }
            }
        }
    } catch (Exception e) {
        // Обработка критических ошибок
        notifyObservers("Критическая ошибка при переводе средств: " + e.getMessage());
    }
}


// ОБМЕН
    public void exchangeCurrency(int cashierId, int clientId, String fromCurrency,
                                 String toCurrency, double amount) {

        Client client = clients.get(clientId);
        if (client != null) {
            synchronized (client) {
                if (client.getCurrency().equals(fromCurrency)) {
                    double convertedAmount = convertCurrency(amount, fromCurrency, toCurrency);
                    client.setCurrency(toCurrency);
                    client.setBalance(convertedAmount);
                    notifyObservers("Касса " + cashierId + ": обмен " + amount + " " +
                            fromCurrency + " на " + convertedAmount + " " + toCurrency +
                            " для клиента " + clientId);
                } else {
                    notifyObservers("Ошибка обмена: валюта клиента не соответствует исходной валюте");
                }
            }
        }
    }

    // Методы получения информации
    // Карта курсов валют, возвращает новую карту курсов, чтобы не дать изменить оригинальные курсы
    public Map<String, Double> getExchangeRates() {
        synchronized (exchangeRates) {
            return new HashMap<>(exchangeRates);
        }
    }
// Список наблюдателей
    public List<Observer> getObservers() {
        return new ArrayList<>(observers);
    }
// Метод удаления наблюдателя
    public void removeObserver(Observer observer) {
        observers.remove(observer);
    }

    // Геттеры для курсов валют
    public double getExchangeRate(String currency) {
        synchronized (exchangeRates) {
            return exchangeRates.getOrDefault(currency, 0.0);
        }
    }

    // Дополнительные методы для работы с транзакциями
    public void addTransaction(Transaction transaction) {
        synchronized (transactionQueue) {
            transactionQueue.add(transaction);
            submittedTransactionsCount.incrementAndGet();
            notifyObservers("Добавлена новая транзакция: " + transaction.getType());
        }
    }
// Очередь транзакций
    public LinkedBlockingQueue<Transaction> getTransactionQueue() {
        return transactionQueue;
    }

// Метод завершения работы банковской системы
    public void shutdown() {
        try {
            notifyObservers("Инициировано завершение работы системы");

            notifyObservers("Транзакций в очереди = "+String.valueOf(transactionQueue.stream().count()));

            // Выводим статистику по обоим счетчикам
            notifyObservers("Всего подано транзакций: " + getSubmittedTransactionsCount());
            notifyObservers("Всего обработано транзакций: " + getProcessedTransactionsCount());

            // Очистка очереди транзакций
            synchronized (transactionQueue) {
                transactionQueue.clear();
            }

            // Остановка касс
            for (Cashier cashier : cashiers) {
                cashier.shutdown();
            }


            // Остановка ExecutorService
            executorService.shutdown();

            // Ждем завершения всех задач с увеличенным таймаутом
            boolean terminated = executorService.awaitTermination(1, TimeUnit.SECONDS);

          //  if (!terminated) {
           //     notifyObservers("Внимание! Некоторые задачи не завершились вовремя");

                // Принудительное завершение
                executorService.shutdownNow();

                // Последний шанс дождаться завершения
         //       if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
          //          notifyObservers("Критическая ошибка: система не может завершить работу");
          //      }
          //  }

            // Добавляем проверку состояния очереди
            if (!transactionQueue.isEmpty()) {
                notifyObservers("В очереди остались необработанные транзакции!");
                for (Transaction transaction : transactionQueue) {
                    notifyObservers("Необработанная транзакция: " + transaction.getType());
                }
            }

            // Очистка ресурсов
            clearResources();
            notifyObservers("Система банка успешно завершила работу");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            notifyObservers("Ошибка при завершении работы: " + e.getMessage());
        }
    }

// освобождаем ресурсы, чистим: клиенты, курсы, транзакции, наблюдатели
    private void clearResources() {
        clients.clear();
        exchangeRates.clear();
        transactionQueue.clear();
        observers.clear();
    }

}

// Реализация класса Logger для отображения в консоли
class Logger implements Observer {
    @Override
    public void update(String message) {
        System.out.println("LOG: " + message);
    }
}
// Пополнение
class DepositTransaction extends Transaction {
    private double amount;

    public DepositTransaction(int clientId, double amount) {
        super(clientId, TransactionType.DEPOSIT);
        this.amount = amount;
    }

    @Override
    public void execute(Bank bank, int cashierId) {
        bank.deposit(cashierId, clientId, amount);
    }
}

// Снятие
class WithdrawTransaction extends Transaction {
    private double amount;

    public WithdrawTransaction(int clientId, double amount) {
        super(clientId, TransactionType.WITHDRAW);
        this.amount = amount;
    }

    @Override
    public void execute(Bank bank, int cashierId) {
        bank.withdraw(cashierId, clientId, amount);
    }
}
// Перевод
class TransferTransaction extends Transaction {
    private int receiverId;
    private double amount;

    public TransferTransaction(int senderId, int receiverId, double amount) {
        super(senderId, TransactionType.TRANSFER);
        this.receiverId = receiverId;
        this.amount = amount;
    }

    @Override
    public void execute(Bank bank, int cashierId) {
        bank.transferFunds(cashierId, getClientId(), receiverId, amount);
    }
}
// Обмен валют
class ExchangeTransaction extends Transaction {
    private String fromCurrency;
    private String toCurrency;
    private double amount;

    public ExchangeTransaction(int clientId, String fromCurrency, String toCurrency, double amount) {
        super(clientId, TransactionType.EXCHANGE);
        this.fromCurrency = fromCurrency;
        this.toCurrency = toCurrency;
        this.amount = amount;
    }

    @Override
    public void execute(Bank bank, int cashierId) {
        bank.exchangeCurrency(cashierId, getClientId(), fromCurrency, toCurrency, amount);
    }
}

// Пример запуска Банковской системы
public class BankApp {
    public static void main(String[] args) {
        int Tcount=0;

        Properties config = new Properties();
        try (InputStream input = BankApp.class.getResourceAsStream("/config.properties")) {
            if (input == null) {
                System.out.println("Конфигурационный файл не найден. Используются курсы по умолчанию.");
                // Устанавливаем дефолтные курсы
                config.put("currency.USD", "1.0");
                config.put("currency.EUR", "0.86");
                config.put("currency.RUB", "80.97");
            } else {
                config.load(input);
            }

            // Создаем банк с указанным количеством касс
            int cashierCount = Integer.parseInt(config.getProperty("bank.cashiers", "3"));
            Bank bank = new Bank(cashierCount);


            // Добавляем наблюдателей
            bank.addObserver(new Logger());

            // Создаем тестовых клиентов
            bank.addClient(new Client(1, 1000, "USD", "Иван Петров"));
            bank.addClient(new Client(2, 1500, "EUR", "Петр Иванов"));
            bank.addClient(new Client(3, 2000, "RUB", "Анна Сидорова"));

            // Создаем и добавляем транзакции в очередь

            // Группа 1: Пополнения
            bank.addTransaction(new DepositTransaction(1, 500));
            bank.addTransaction(new DepositTransaction(2, 700));
            bank.addTransaction(new DepositTransaction(3, 1000));


            //Группа 2: Снятия
            bank.addTransaction(new WithdrawTransaction(1, 30000));
            bank.addTransaction(new WithdrawTransaction(2, 200));

            //Группа 3: Переводы
            bank.addTransaction(new TransferTransaction(1, 2, 400));
            bank.addTransaction(new TransferTransaction(3, 1, 50000));

            //Группа 4: Обмены валют
            bank.addTransaction(new ExchangeTransaction(1, "USD", "EUR", 200));
            bank.addTransaction(new ExchangeTransaction(2, "EUR", "RUB", 300));

            //Массовое добавление клиентов и пополнения счета
            for (int i = 4; i <= 10; i++) {
                bank.addClient(new Client(i, i*100, "RUB", "Клиент " + i));
                bank.addTransaction(new DepositTransaction(i, 200));
            }

            // Ждем завершения всех операций
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Завершаем работу
            bank.shutdown();

        } catch (IOException e) {
            System.err.println("Ошибка загрузки конфигурации: " + e.getMessage());
        }
    }
}