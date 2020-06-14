@file:JvmName("RunCorrectScenario")

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.scheduling.*
import kotlin.random.*

@InternalCoroutinesApi
private val context = ExperimentalCoroutineDispatcher(corePoolSize = 2, maxPoolSize = 2)

/**
 * Проверяем, что для распределенной системы, где все процессы корректные, предложенные алгоритмы работают.
 * Для этого для каждого участника системы:
 *  1. Запускаем процесс (в нашем случае корутину), который в while(true) пытается переводить рандомную сумму денег
 *  2. Ждем несколько секунд
 *  3. Останавливаем корутину
 * Сначала мы проверяем, что все корутины действительно остановятся: это значит, что в этой системе кворумов
 * есть живучесть.
 * После этого проверяем, что изначальная сумма денег, распределенная по аккаунтам, равна итоговой сумме денег,
 * распределенной по аккаунтам. То есть деньги никуда не пропали и не появились из воздуха. То же самое проверяем для
 * локального виденья состояния системы каждым из участников.
 */
@InternalCoroutinesApi
fun main() {
    val symmetricDistributedSystem = SymmetricDistributedSystem()
    val totalBalance = symmetricDistributedSystem.getBalances().map { it.value }.sum()
    val processes = symmetricDistributedSystem
            .getProcesses()
            .map {
                val c = Cryptocurrency(symmetricDistributedSystem, it)
                Process(it, c, symmetricDistributedSystem.getProcesses(), totalBalance)
            }
            .toList()

    processes.forEach { it.startScenario() }
    println("Started all processes")

    Thread.sleep(2000)

    processes.forEach { it.stopScenario() }
    println("Stopped all processes")

    runBlocking {
        for (process in processes) {
            process.runCoroutine.join()
        }
        val currentTotalBalance = processes.map{ it.getBalance() }.sum()

        check(currentTotalBalance == totalBalance)
        println("Current total balance $currentTotalBalance")
        processes.forEach { println("process ${it.processId} has balance ${it.getBalance()}") }

        processes.forEach { process ->
            val processBalances = processes.map { process.getBalance(it.processId) }.sum()
            check(processBalances == totalBalance)
        }
    }
}

private class Process(val processId: processId,
                      private val cryptocurrency: Cryptocurrency,
                      processes: Set<processId>,
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

    suspend fun getBalance() = cryptocurrency.read(processId)

    suspend fun getBalance(processId: processId) = cryptocurrency.read(processId)

    fun stopScenario() {
        stopped = true
    }
}

/**
 * Распределенная система с симметричной системой кворумов, в которой кворумы описаны множествами процессов,
 * построенными на кворумных слайсах.
 */
private class SymmetricDistributedSystem : DistributedSystem<ChannelMessage> {
    private val channels = HashMap<processId, Channel<ChannelMessage>>()
    private val qs: MutableMap<processId, FBQS> = HashMap()
    private val balances: MutableMap<processId, money> = HashMap()

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

    override fun getProcesses(): Set<processId> {
        return channels.keys
    }

    override fun getChannels(): Map<processId, Channel<ChannelMessage>> {
        return channels
    }

    override fun getQuorumSystem(): Map<processId, FBQS> {
        return qs
    }

    override fun getBalances(): Map<processId, Int> {
        return balances
    }

    private inner class SymmetricFBQS : FBQS {
        // система кворумов, построенная по кворумным слайсам:
        // {{0, 1}, {0, 1, 2}, {0, 2, 3}, {0, 1, 2, 3}}
        private val q1 = setOf(0, 1)
        private val q2 = setOf(0, 1, 2)
        private val q3 = setOf(0, 2, 3)
        private val q4 = setOf(0, 1, 2, 3)

        override fun hasQuorum(process: processId, processes: Set<processId>): Boolean {
            return when (process) {
                0 -> processes.containsAll(q1) || processes.containsAll(q2) || processes.containsAll(q3) || processes.containsAll(q4)
                1 -> processes.containsAll(q1) || processes.containsAll(q2) || processes.containsAll(q4)
                2 -> processes.containsAll(q2) || processes.containsAll(q3) || processes.containsAll(q4)
                3 -> processes.containsAll(q3) || processes.containsAll(q4)
                else -> false
            }
        }

        override fun hasBlockingSet(process: processId, processes: Set<processId>): Boolean {
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
class MajoritySymmetricDistributedSystem(private val numberOfProcesses: Int = 20) : DistributedSystem<ChannelMessage> {
    private val channels: MutableMap<processId, Channel<ChannelMessage>> = HashMap()
    private val qs: MutableMap<processId, FBQS> = HashMap()
    private val balances: MutableMap<processId, money> = HashMap()

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

    override fun getProcesses(): Set<processId> = channels.keys

    override fun getChannels(): Map<processId, Channel<ChannelMessage>> = channels

    override fun getQuorumSystem(): Map<processId, FBQS> = qs

    override fun getBalances(): Map<processId, money> = balances

    private inner class SymmetricFBQS : FBQS {
        override fun hasQuorum(process: processId, processes: Set<processId>): Boolean {
            return processes.contains(process) && processes.size > numberOfProcesses / 2
        }

        override fun hasBlockingSet(process: processId, processes: Set<processId>): Boolean {
            return processes.size > numberOfProcesses / 2
        }
    }
}
