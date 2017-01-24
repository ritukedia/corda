package net.corda.core.contracts

import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.SecureHash
import net.corda.core.transactions.WireTransaction

// The dummy contract doesn't do anything useful. It exists for testing purposes.
val DUMMY_V2_PROGRAM_ID = DummyContractV2()

/**
 * Dummy contract state upgrade logic for testing of the upgrade process.
 */
class DummyContractUpgrade : ContractUpgrade<DummyContract.State, DummyContractV2.State> {
    override val legacyContract: Contract get() = DUMMY_PROGRAM_ID
    override val upgradedContract: Contract get() = DUMMY_V2_PROGRAM_ID
    override fun upgrade(state: DummyContract.State): DummyContractV2.State {
        return DummyContractV2.State(state.magicNumber, state.participants)
    }
}

class DummyContractV2 : Contract {
    data class State(val magicNumber: Int = 0, val owners: List<CompositeKey>) : ContractState {
        override val contract = DUMMY_V2_PROGRAM_ID
        override val participants: List<CompositeKey> = owners
    }

    interface Commands : CommandData {
        class Create : TypeOnlyCommandData(), Commands
        class Move : TypeOnlyCommandData(), Commands
    }

    override fun verify(tx: TransactionForContract) {}
    // The "empty contract"
    override val legalContractReference: SecureHash = SecureHash.sha256("")

    /**
     * Generate an upgrade transaction from [DummyContract].
     *
     * @return a pair of wire transaction, and a set of those who should sign the transaction for it to be valid.
     */
    fun generateUpgradeFromV1(vararg states: StateAndRef<DummyContract.State>): Pair<WireTransaction, Set<CompositeKey>> {
        val notary = states.map { it.state.notary }.single()
        require(states.isNotEmpty())

        val signees = states.flatMap { it.state.data.participants }.toSet()
        return Pair(TransactionType.General.Builder(notary).apply {
            states.forEach {
                addInputState(it)
                addOutputState(DummyContractUpgrade().upgrade(it.state.data))
                addCommand(UpgradeCommand(DummyContractUpgrade()), signees.toList())
            }
        }.toWireTransaction(), signees)
    }
}
