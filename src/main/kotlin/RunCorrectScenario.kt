@file:JvmName("RunCorrectScenario")

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.scheduling.*
import kotlin.collections.HashMap
import kotlin.random.Random

@InternalCoroutinesApi
private val context = ExperimentalCoroutineDispatcher(corePoolSize = 2, maxPoolSize = 2)

/**
 * Options:
 *  1. SymmetricDistributedSystem
 *  2. MajoritySymmetricDistributedSystem
 */
private val dsCreator: () -> DistributedSystem<Channel<ChannelMessage>> = { SymmetricDistributedSystem() }

/**
 * Проверяем, что для распределенной системы, где все процессы корректные, предложенные алгоритмы работают.
 * Для этого для каждого участника системы:
 *  1. Запускаем процесс (в нашем случае корутину), который в while(true) пытается переводить рандомную сумму денег
 *  2. Ждем несколько секунд
 *  3. Останавливаем корутину
 * Сначала мы проверяем, что все корутины действительно остановятся: это значит, что в этой системе кворумов
 * есть живучесть.
 * После этого проверяем, что изначальная сумма денег, распределенная по аккаунтам, равна итоговой сумме денег,
 * распределенной по аккаунтам. То есть деньги никуда не пропали и не появились из воздуха.
 * После этого строим глобальную упорядоченную историю по зависимостям транзакции методом [getTotalOrderedHistory].
 * Выполняем ее последовательно, убеждаемся, что в любой момент времени баланс не меньше нуля.
 */
@InternalCoroutinesApi
fun main() {
    val distributedSystem = dsCreator.invoke()
    val totalBalance = distributedSystem.getBalances().map { it.value }.sum()
    val processes = distributedSystem
            .getProcesses()
            .map {
                val c = Cryptocurrency(distributedSystem, it)
                CorrectProcess(it, c, distributedSystem.getProcesses(), totalBalance)
            }
            .toList()

    processes.forEach { it.startScenario() }
    println("Started all processes")

    Thread.sleep(2000)

    processes.forEach { it.stopScenario() }
    println("Stopped all processes")

    checkLiveness(processes)
    checkSafety(processes, distributedSystem)
}

private fun checkLiveness(processes: List<CorrectProcess>) {
    runBlocking {
        for (process in processes) {
            process.runCoroutine.join()
        }
    }
}

private fun checkSafety(processes: List<CorrectProcess>, distributedSystem: DistributedSystem<Channel<ChannelMessage>>) {
    val initialBalance = distributedSystem.getBalances().map { it.value }.sum()
    val currentTotalBalance = processes.map{ it.getBalance() }.sum()

    // Проверяем, что деньги не испарились или не появились из воздуха
    check(currentTotalBalance == initialBalance)
    println("Current total balance $currentTotalBalance")
    processes.forEach { println("process ${it.processId} has balance ${it.getBalance()}") }

    // Проверяем, что у каждого процесса локально деньги не испарились или не появились из воздуха
    processes.forEach { process ->
        val processBalances = processes.map { process.getBalance(it.processId) }.sum()
        check(processBalances == initialBalance)
    }

    // Получаем тотально упорядоченную историю
    val history = getTotalOrderedHistory(processes.map { it.cryptocurrency }.toTypedArray())

    val cryptocurrencyObject = IntArray(distributedSystem.getProcesses().size)
    repeat(distributedSystem.getProcesses().size) {
        cryptocurrencyObject[it] = distributedSystem.getBalance(it)
    }
    // Выполняем успешные операции перевода над объектом критовалюты в порядке тотально упорядоченной истории.
    // Проверяем, что очередная транзакция не нарушает требование на неотрицательность аккаунта.
    for (transfer in history) {
        cryptocurrencyObject[transfer.sender] -= transfer.transferValue
        cryptocurrencyObject[transfer.receiver] += transfer.transferValue
        check(cryptocurrencyObject[transfer.sender] >= 0)
    }
    processes.forEach { check(cryptocurrencyObject[it.processId] == it.getBalance()) }
}

class CorrectProcess(val processId: ProcessId,
                     val cryptocurrency: Cryptocurrency,
                     processes: Set<ProcessId>,
                     private val totalBalance: Int) {
    private val otherProcesses = processes.filter { it != processId }.toList()

    @Volatile
    private var stopped = false

    lateinit var runCoroutine: Job

    @InternalCoroutinesApi
    fun startScenario() {
        runCoroutine = CoroutineScope(context).launch {
            while (!stopped) {
                val receiver = otherProcesses.random()
                val transferValue = Random.nextInt(totalBalance)
                cryptocurrency.transfer(receiver, transferValue)
                delay(Random.nextInt(100).toLong())
            }
        }
    }

    fun getBalance() = runBlocking { cryptocurrency.read(processId) }

    fun getBalance(processId: ProcessId) = runBlocking { cryptocurrency.read(processId) }

    fun stopScenario() {
        stopped = true
    }
}

/**
 * Распределенная система с симметричной системой кворумов, в которой кворумы описаны множествами процессов,
 * построенными на кворумных слайсах.
 */
private class SymmetricDistributedSystem : DistributedSystem<Channel<ChannelMessage>> {
    private val channels = HashMap<ProcessId, Channel<ChannelMessage>>()
    private val qs: MutableMap<ProcessId, FBQS> = HashMap()
    private val balances: MutableMap<ProcessId, Money> = HashMap()

    init {
        repeat(4) {
            channels[it] = Channel(Channel.UNLIMITED)
        }
        repeat(4) {
            qs[it] = SymmetricFBQS()
        }
        repeat(4) {
            balances[it] = 100
        }
    }

    override fun getProcesses(): Set<ProcessId> {
        return channels.keys
    }

    override fun getChannels(): Map<ProcessId, Channel<ChannelMessage>> {
        return channels
    }

    override fun getQuorumSystem(): Map<ProcessId, FBQS> {
        return qs
    }

    override fun getBalances(): Map<ProcessId, Int> {
        return balances
    }

    private inner class SymmetricFBQS : FBQS {
        // система кворумов, построенная по кворумным слайсам:
        // {{0, 1}, {0, 1, 2}, {0, 2, 3}, {0, 1, 2, 3}}
        private val q1 = setOf(0, 1)
        private val q2 = setOf(0, 1, 2)
        private val q3 = setOf(0, 2, 3)
        private val q4 = setOf(0, 1, 2, 3)

        override fun hasQuorum(process: ProcessId, processes: Set<ProcessId>): Boolean {
            return when (process) {
                0 -> processes.containsAll(q1) || processes.containsAll(q2) || processes.containsAll(q3) || processes.containsAll(q4)
                1 -> processes.containsAll(q1) || processes.containsAll(q2) || processes.containsAll(q4)
                2 -> processes.containsAll(q2) || processes.containsAll(q3) || processes.containsAll(q4)
                3 -> processes.containsAll(q3) || processes.containsAll(q4)
                else -> false
            }
        }

        override fun hasBlockingSet(process: ProcessId, processes: Set<ProcessId>): Boolean {
            // Кворумные слайсы:
            // S(0) = {{0, 1}, {0, 3}}
            // S(1) = {{0, 1}}
            // S(2) = {{0, 2}}
            // S(3) = {{2, 3}}
            return when (process) {
                0 -> processes.contains(1) && processes.contains(3)
                1 -> processes.contains(0)
                2 -> processes.contains(0)
                3 -> processes.contains(2)
                else -> false
            }
        }
    }
}

/**
 * Распределенная система с "мажоритарной" системой кворумов: в ней кворумом считается больше половины процессов.
 */
class MajoritySymmetricDistributedSystem(private val numberOfProcesses: Int = 20) : DistributedSystem<Channel<ChannelMessage>> {
    private val channels: MutableMap<ProcessId, Channel<ChannelMessage>> = HashMap()
    private val qs: MutableMap<ProcessId, FBQS> = HashMap()
    private val balances: MutableMap<ProcessId, Money> = HashMap()

    init {
        repeat(numberOfProcesses) {
            channels[it] = Channel(Channel.UNLIMITED)
        }
        repeat(numberOfProcesses) {
            qs[it] = SymmetricFBQS()
        }
        repeat(numberOfProcesses) {
            balances[it] = 100
        }
    }

    override fun getProcesses(): Set<ProcessId> = channels.keys

    override fun getChannels(): Map<ProcessId, Channel<ChannelMessage>> = channels

    override fun getQuorumSystem(): Map<ProcessId, FBQS> = qs

    override fun getBalances(): Map<ProcessId, Money> = balances

    private inner class SymmetricFBQS : FBQS {
        override fun hasQuorum(process: ProcessId, processes: Set<ProcessId>): Boolean {
            return processes.contains(process) && processes.size > numberOfProcesses / 2
        }

        override fun hasBlockingSet(process: ProcessId, processes: Set<ProcessId>): Boolean {
            return processes.size > numberOfProcesses / 2
        }
    }
}
