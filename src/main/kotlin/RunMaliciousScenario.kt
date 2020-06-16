@file:JvmName("RunMaliciousScenario")

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.scheduling.*

@InternalCoroutinesApi
private val context = ExperimentalCoroutineDispatcher(corePoolSize = 2, maxPoolSize = 2)

/**
 * Проверяем, что для распределенной системы, где все процессы корректные, есть отдельные нетронутые множества, и есть
 * один некорректный процесс, предложенные алгоритмы работают так, как ожидается.
 * Для этого для каждого участника системы:
 *  1. Запускаем процесс (в нашем случае корутину), который в while(true) пытается переводить рандомную сумму денег
 *  2. Ждем несколько секунд
 *  3. Останавливаем корутину
 * Сначала мы проверяем, что все корутины действительно остановятся: это значит, что в этой системе кворумов для нетронутых
 * множеств есть живучесть.
 * После этого проверяем, что для каждого нетронутого множества глобальная упорядоченная история, построенная
 * по зависимостям одобреных им транзакций, корректна, то есть в любой момент времени баланс не меньше нуля.
 * Однако глобально история, построенная по всем одобренным транзакциям корректных процессов, с большой вероятностью
 * не будет корректной.
 */
@InternalCoroutinesApi
fun main() {
    val intactSets = 4
    val distributedSystem = SymmetricDistributedSystemWithoutQI(intactSets)
    val totalBalance = distributedSystem.getBalances().map { it.value }.sum()
    val processes = distributedSystem
            .getProcesses()
            .filter { it != distributedSystem.getMaliciousProcessId() }
            .map {
                val c = Cryptocurrency(distributedSystem, it)
                CorrectProcess(it, c, distributedSystem.getProcesses(), totalBalance)
            }
            .toList()

    val maliciousProcess = MaliciousProcess(distributedSystem)
    maliciousProcess.startScenario()
    processes.forEach { it.startScenario() }
    println("Started all processes")

    Thread.sleep(2000)

    processes.forEach { it.stopScenario() }
    println("Stopped all processes")

    checkLiveness(processes)
    checkSafety(processes, distributedSystem)

    // Проверяем, все ли ок глобально (с большой вероятностью в этом случае все будет не ок).
    try {
        checkForSpecificProcesses(distributedSystem,
                distributedSystem.getProcesses().filter { it != distributedSystem.getMaliciousProcessId() }.toSet(), processes)
        println("Cryptocurrency is correct for all correct processes")
    }
    catch (e: IllegalStateException) {
        println("Cryptocurrency is not globally correct")
    }
}

private fun checkLiveness(processes: List<CorrectProcess>) {
    runBlocking {
        for (process in processes) {
            process.runCoroutine.join()
        }
    }
}

/**
 * Проверяем, что для каждого нетронутого множества все ок.
 */
private fun checkSafety(processes: List<CorrectProcess>, distributedSystem: SymmetricDistributedSystemWithoutQI) {
    repeat(distributedSystem.intactSets) {
        checkForSpecificProcesses(distributedSystem, getIntactSet(it), processes)
    }
}

private fun checkForSpecificProcesses(distributedSystem: SymmetricDistributedSystemWithoutQI,
                                      processesToCheck: Set<ProcessId>,
                                      processes: List<CorrectProcess>) {
    val cryptocurrencies = processesToCheck
            .map { i -> processes[i].cryptocurrency }.toTypedArray()
    // Получаем тотально упорядоченную историю
    val history = getTotalOrderedHistory(cryptocurrencies)

    val cryptocurrencyObject = IntArray(distributedSystem.getProcesses().size)
    repeat(distributedSystem.getProcesses().size) { process ->
        cryptocurrencyObject[process] = distributedSystem.getBalance(process)
    }
    // Выполняем успешные операции перевода над объектом критовалюты в порядке тотально упорядоченной истории.
    // Проверяем, что очередная транзакция не нарушает требование на неотрицательность аккаунта.
    for (transfer in history) {
        cryptocurrencyObject[transfer.sender] -= transfer.transferValue
        cryptocurrencyObject[transfer.receiver] += transfer.transferValue
        check(cryptocurrencyObject[transfer.sender] >= 0)
    }
    processesToCheck.forEach { id -> check(cryptocurrencyObject[id] == processes[id].getBalance()) }
}

private class MaliciousProcess(val distributedSystem: SymmetricDistributedSystemWithoutQI) {
    lateinit var runCoroutine: Job

    @InternalCoroutinesApi
    fun startScenario() {
        runCoroutine = CoroutineScope(context).launch {
            val balance = distributedSystem.getMaliciousBalance()
            repeat(balance / 10) {
                repeat(distributedSystem.intactSets) { trId ->
                    sendMaliciousMessages(trId, distributedSystem, true, 10, it + 1)
                }
            }
        }
    }
}

/**
 * Симметричная система кворумов без требования о пересечении кворумов.
 * Здесь нетронутые
 */
class SymmetricDistributedSystemWithoutQI(val intactSets: Int = 1) : DistributedSystem<Channel<ChannelMessage>> {
    private val channels: MutableMap<ProcessId, Channel<ChannelMessage>> = HashMap()
    private val qs: MutableMap<ProcessId, FBQS> = HashMap()
    private val balances: MutableMap<ProcessId, Money> = HashMap()

    init {
        repeat(intactSets * 2) {
            channels[it] = Channel(Channel.UNLIMITED)
        }
        channels[intactSets * 2] = Channel(Channel.UNLIMITED)
        repeat(intactSets * 2) {
            qs[it] = SymmetricFBQS()
        }
        qs[intactSets * 2] = SymmetricFBQS()
        repeat(intactSets * 2) {
            balances[it] = 100
        }
        balances[intactSets * 2] = 100
    }

    override fun getProcesses(): Set<ProcessId> = channels.keys

    override fun getChannels(): Map<ProcessId, Channel<ChannelMessage>> = channels

    override fun getQuorumSystem(): Map<ProcessId, FBQS> = qs

    override fun getBalances(): Map<ProcessId, Money> = balances

    fun getMaliciousProcessId(): ProcessId = intactSets * 2

    private inner class SymmetricFBQS : FBQS {
        override fun hasQuorum(process: ProcessId, processes: Set<ProcessId>): Boolean {
            var hasQuorum = false
            repeat(intactSets) {
                hasQuorum = hasQuorum || processes.containsAll(setOf(it * 2, it * 2 + 1)) && (process == it * 2 || process == it * 2 + 1)
            }
            return hasQuorum
        }

        override fun hasBlockingSet(process: ProcessId, processes: Set<ProcessId>): Boolean {
            var hasBlockingSet = false
            repeat(intactSets) {
                hasBlockingSet = hasBlockingSet || processes.contains(it * 2) && process == it * 2 + 1 ||
                        processes.contains(it * 2 + 1) && process == it * 2
            }
            return hasBlockingSet
        }
    }
}

private fun SymmetricDistributedSystemWithoutQI.getMaliciousBalance() = getBalance(getMaliciousProcessId())

private fun getIntactSet(intactSetNumber: Int): Set<ProcessId>
        = setOf(intactSetNumber * 2, intactSetNumber * 2 + 1)

suspend fun sendMaliciousMessages(intactSetNumber: Int, ds: SymmetricDistributedSystemWithoutQI, sameMessagesForIntactSet: Boolean,
                                  transferValue: Money = 10, transferId: Int = 1) {
    val same = if (sameMessagesForIntactSet) 0 else 1
    val maliciousProcessId = ds.getMaliciousProcessId()
    repeat(intactSetNumber) {
        ds.getChannel(it * 2).send(Bcast(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2, transferValue, transferId), setOf())))
        ds.getChannel(it * 2 + 1).send(Bcast(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2 + same, transferValue, transferId), setOf())))
    }
    repeat(intactSetNumber) {
        ds.getChannel(it * 2).send(Echo(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2, transferValue, transferId), setOf())))
        ds.getChannel(it * 2 + 1).send(Echo(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2 + same, transferValue, transferId), setOf())))
    }
    repeat(intactSetNumber) {
        ds.getChannel(it * 2).send(Ready(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2, transferValue, transferId), setOf())))
        ds.getChannel(it * 2 + 1).send(Ready(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2 + same, transferValue, transferId), setOf())))
    }
}