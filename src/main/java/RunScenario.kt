import kotlinx.coroutines.*
import kotlinx.coroutines.scheduling.*
import kotlin.random.*

@InternalCoroutinesApi
private val context = ExperimentalCoroutineDispatcher(corePoolSize = 2, maxPoolSize = 2)

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
            println("Joining ${process.processId} process")
            process.runCoroutine.join()
        }
        val currentTotalBalance = processes.map{ it.getBalance() }.sum()

        print("Current total balance $currentTotalBalance")
        check(currentTotalBalance <= totalBalance)
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
            if (stopped) return@launch
            val receiver = otherProcesses.random()
            val transferValue = Random.nextInt(totalBalance)
            cryptocurrency.transfer(receiver, transferValue)
            delay(Random.nextInt(100).toLong())
        }
    }

    suspend fun getBalance() = cryptocurrency.read(processId)

    fun stopScenario() {
        stopped = true
    }
}