import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.*
import org.junit.*
import org.junit.jupiter.api.Assertions.*

class CryptocurrencyTest {
    /**
     * Тест, что если процесс начал корректную транзакцию, то он ее закончит, если все остальные процессы корректные.
     * Балансы аккаунтов изменятся соответственно.
     */
    @Test
    fun testSuccessfulTransfer() = runBlocking {
        val ds = MajoritySymmetricDistributedSystem(3)
        val cryptocurrency = Cryptocurrency(ds, 0)
        GlobalScope.launch {
            while (true) {
                select<Unit> {
                    ds.getChannel(1).onReceive { sendAnswers(1, it, ds.getChannel(0)) }
                    ds.getChannel(2).onReceive { sendAnswers(2, it, ds.getChannel(0)) }
                }
            }
        }
        val successful = cryptocurrency.transfer(1, 10)
        assertTrue(successful)
        assertTrue(cryptocurrency.read(0) == ds.getBalance(0) - 10)
        assertTrue(cryptocurrency.read(1) == ds.getBalance(1) + 10)
    }

    /**
     * Тест, что если процесс начал некорректную транзакцию, то она не будет проведена. Балансы аккаунтов не изменятся.
     */
    @Test
    fun testFailTransfer() = runBlocking {
        val ds = MajoritySymmetricDistributedSystem(3)
        val cryptocurrency = Cryptocurrency(ds, 0)
        GlobalScope.launch {
            while (true) {
                select<Unit> {
                    ds.getChannel(1).onReceive { sendAnswers(1, it, ds.getChannel(0)) }
                    ds.getChannel(2).onReceive { sendAnswers(2, it, ds.getChannel(0)) }
                }
            }
        }
        val successful = cryptocurrency.transfer(1, 110)
        assertTrue(!successful)
        assertTrue(cryptocurrency.read(0) == ds.getBalance(0))
        assertTrue(cryptocurrency.read(1) == ds.getBalance(1))
    }

    /**
     * Тест, что если некоторый процесс отправил информацию о транзакции, то процесс ее увидит, если все остальные
     * процессы корректные. Балансы аккаунтов изменятся соответсственно.
     */
    @Test
    fun testSuccessfulTransferred() = runBlocking {
        val ds = MajoritySymmetricDistributedSystem(3)
        val cryptocurrency = Cryptocurrency(ds, 0)
        GlobalScope.launch {
            while (true) {
                select<Unit> {
                    ds.getChannel(1).onReceive { sendAnswers(1, it, ds.getChannel(0)) }
                    ds.getChannel(2).onReceive { sendAnswers(2, it, ds.getChannel(0)) }
                }
            }
        }
        ds.getChannel(0).send(Bcast(1, Message(TransactionInfo(1, 0, 10, 1), setOf())))
        delay(100)
        assertTrue(cryptocurrency.read(0) == ds.getBalance(0) + 10)
        assertTrue(cryptocurrency.read(1) == ds.getBalance(1) - 10)
    }

    /**
     * Тест, что если некоторый процесс отправил информацию о транзакции, но процесс не видел транзакцию до нее, то
     * процесс ее не применит.
     */
    @Test
    fun testTransferredUnknownTransferId() = runBlocking {
        val ds = MajoritySymmetricDistributedSystem(3)
        val cryptocurrency = Cryptocurrency(ds, 0)
        GlobalScope.launch {
            while (true) {
                select<Unit> {
                    ds.getChannel(1).onReceive { sendAnswers(1, it, ds.getChannel(0)) }
                    ds.getChannel(2).onReceive { sendAnswers(2, it, ds.getChannel(0)) }
                }
            }
        }
        ds.getChannel(0).send(Bcast(1, Message(TransactionInfo(1, 0, 10, 2), setOf())))
        delay(100)
        assertTrue(cryptocurrency.read(0) == ds.getBalance(0))
        assertTrue(cryptocurrency.read(1) == ds.getBalance(1))
    }

    /**
     * Тест, что для переплетенных процессов не могут пройти две разные транзакции.
     */
    @InternalCoroutinesApi
    @Test
    fun testTotalityIntactSets() = runBlocking{
        val intactSets = 1
        val ds = SymmetricDistributedSystemWithoutQI(intactSets)
        val cryptocurrencies = ArrayList<Cryptocurrency>()
        repeat(intactSets) {
            cryptocurrencies += Cryptocurrency(ds, it * 2)
            cryptocurrencies += Cryptocurrency(ds, it * 2 + 1)
        }
        val initialSum = cryptocurrencies.map { cryptocurrency -> cryptocurrency.read(cryptocurrency.process) }.sum()
        sendMaliciousMessages(intactSets,  ds, false)
        delay(100)
        val sum = cryptocurrencies.map { cryptocurrency -> cryptocurrency.read(cryptocurrency.process) }.sum()
        assertTrue(sum == initialSum)
    }

    /**
     * Тест, что деньги могут потратиться не более K раз (K – количество покрытий кликами графа на процессах.
     * В описанной FBQS в [SymmetricDistributedSystemWithoutQI] нетронутое множество будет одной кликой, то есть покрытие будет
     * числом нетронутых множеств кликами.
     */
    @InternalCoroutinesApi
    @Test
    fun testKSpending() = runBlocking{
        val intactSets = 4
        val ds = SymmetricDistributedSystemWithoutQI(intactSets)
        val cryptocurrencies = ArrayList<Cryptocurrency>()
        repeat(intactSets) {
            cryptocurrencies += Cryptocurrency(ds, it * 2)
            cryptocurrencies += Cryptocurrency(ds, it * 2 + 1)
        }
        val initialSum = cryptocurrencies.map { cryptocurrency -> cryptocurrency.read(cryptocurrency.process) }.sum()
        sendMaliciousMessages(intactSets,  ds, true)
        delay(300)
        val sum = cryptocurrencies.map { cryptocurrency -> cryptocurrency.read(cryptocurrency.process) }.sum()
        assertTrue(sum <= initialSum + 10 * intactSets)
    }

    private suspend fun sendAnswers(process: ProcessId, m: ChannelMessage, c: Channel<ChannelMessage>) {
        when (m) {
            is Bcast -> c.send(Echo(process, m.message))
            is Echo -> c.send(Ready(process, m.message))
        }
    }
}