fun getTotalOrderedHistory(cryptocurrencies: Array<Cryptocurrency>): List<TransactionInfo> {
    val unorderedHistory = cryptocurrencies.flatMap { getHistory(it) }.toSet()
    return getOrderedHistory(unorderedHistory)
}

/**
 * Упорядочивание истории в соответствии зависимостями транзакций. Некоторый аналог топологической сортировки.
 */
private fun getOrderedHistory(unorderedHistory: Set<Message>): List<TransactionInfo> {
    val history = ArrayList<TransactionInfo>()

    // сообщения, которые нужно обработать
    val messageToProcess = HashSet(unorderedHistory)
    // уже добавленные в историю сообщения
    val processed = HashSet<TransactionInfo>()
    // сокращенное представление транзакций в полное их представление
    val transferToMessage = unorderedHistory.map { Pair(Transfer(it.sender(), it.transferId()), it) }.toMap()
    while (true) {
        val iterator = messageToProcess.iterator()
        while (iterator.hasNext()) {
            val message = iterator.next()
            val transfer = message.transactionInfo
            val prevTransfer = Transfer(transfer.sender, transfer.transferId - 1)
            val containsPrevTransfer = processedPrevTransfer(transfer, transferToMessage[prevTransfer], processed)
            // если, во-первых, все зависимости транзакции уже добавлены, и во-вторых, прошлая транзакция также была добавлена
            if (processed.containsAll(message.deps) && containsPrevTransfer) {
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

private fun processedPrevTransfer(transfer: TransactionInfo, messageWithPrevTransfer: Message?, processed: Set<TransactionInfo>)
        = transfer.transferId == 1 || processed.contains(messageWithPrevTransfer!!.transactionInfo)

private data class Transfer(val sender: ProcessId, val transferId: Int)

private fun getHistory(cryptocurrency: Cryptocurrency): Set<Message> {
    val cryptocurrencyClass = Cryptocurrency::class.java
    val validatedMessages = cryptocurrencyClass.getDeclaredField("validatedMessages")
    validatedMessages.isAccessible = true
    @Suppress("UNCHECKED_CAST")
    return validatedMessages.get(cryptocurrency) as Set<Message>
}
