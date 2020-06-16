fun getTotalOrderedHistory(cryptocurrencies: Array<Cryptocurrency>): List<TransactionInfo> {
    val unorderedHistory = cryptocurrencies.flatMap { getHistory(it) }.toSet()
    val dependencies = getDependenciesForTransaction(unorderedHistory)
    return getOrderedHistory(unorderedHistory, dependencies)
}

/**
 * Упорядочивание истории в соответствии зависимостями транзакций. Некоторый аналог топологической сортировки.
 */
private fun getOrderedHistory(unorderedHistory: Set<Message>,
                              dependencies: Map<TransactionInfo, Set<TransactionInfo>>): List<TransactionInfo> {
    val history = ArrayList<TransactionInfo>()
    val processed = HashSet<TransactionInfo>()
    val transferToMessage = unorderedHistory.map { Pair(Transfer(it.sender(), it.transferId()), it) }.toMap()
    val messageToProcess = HashSet(unorderedHistory.map { it.transactionInfo })

    while (true) {
        val iterator = messageToProcess.iterator()
        while (iterator.hasNext()) {
            val transfer = iterator.next()
            val prevTransfer = Transfer(transfer.sender, transfer.transferId - 1)
            val containsPrevTransfer = transfer.transferId == 1 || processed.contains(transferToMessage[prevTransfer]!!.transactionInfo)
            if (processed.containsAll(dependencies[transfer]!!) && containsPrevTransfer) {
                history += transfer
                processed += transfer
                iterator.remove()
            }
        }
        if (messageToProcess.isEmpty()) {
            break
        }
    }
    return history
}

private data class Transfer(val sender: ProcessId, val transferId: Int)

/**
 * Получение полного списка зависимостей для каждой транзакции.
 */
private fun getDependenciesForTransaction(unorderedHistory: Set<Message>): Map<TransactionInfo, Set<TransactionInfo>> {
    val transactionToDeps = unorderedHistory.map { Pair(it.transactionInfo, it.deps) }.toMap()
    val dependencies = HashMap<TransactionInfo, MutableSet<TransactionInfo>>()
    for (message in unorderedHistory) {
        recursiveDepsSearch(message.transactionInfo, dependencies, transactionToDeps)
    }
    return dependencies
}

/**
 * Поиск в глубину по дереву зависимостей для транзакции. Для транзакции составляется полный список ее зависимостей
 * и зависимостей ее зависимостей.
 */
private fun recursiveDepsSearch(transaction: TransactionInfo,
                                dependencies: HashMap<TransactionInfo, MutableSet<TransactionInfo>>,
                                transactionToDeps: Map<TransactionInfo, Set<TransactionInfo>>) {
    if (dependencies.contains(transaction)) return

    dependencies[transaction] = HashSet()
    val deps = transactionToDeps[transaction]!!
    for (dep in deps) {
        if (dependencies[dep] == null) {
            recursiveDepsSearch(dep, dependencies, transactionToDeps)
        }
        dependencies[transaction]!! += dependencies[dep]!!
        dependencies[transaction]!! += dep
    }
}

private fun getHistory(cryptocurrency: Cryptocurrency): Set<Message> {
    val cryptocurrencyClass = Cryptocurrency::class.java
    val validatedMessages = cryptocurrencyClass.getDeclaredField("validatedMessages")
    validatedMessages.isAccessible = true
    @Suppress("UNCHECKED_CAST")
    return validatedMessages.get(cryptocurrency) as Set<Message>
}
