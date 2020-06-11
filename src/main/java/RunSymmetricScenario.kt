@file:JvmName("RunSymmetricScenario")

import kotlinx.coroutines.*
import kotlinx.coroutines.scheduling.*
import kotlin.random.*

@InternalCoroutinesApi
private val context = ExperimentalCoroutineDispatcher(corePoolSize = 2, maxPoolSize = 2)

/**
 * Проверяем, что для симметричной системы кворумов предложенные алгоритмы работают.
 * Для этого для каждого участника системы:
 *  1. Запускаем процесс (в нашем случае корутину), который в while(true) пытается переводить рандомную сумму денег
 *  2. Ждем несколько секунд
 *  3. Останавливаем корутину
 * Сначала мы проверяем, что все корутины действительно остановятся: это значит, что в симметричной системе кворумов
 * для корректных участников есть живучесть.
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