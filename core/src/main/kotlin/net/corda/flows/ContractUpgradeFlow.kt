package net.corda.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.*
import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.Party
import net.corda.core.flows.FlowLogic
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.UntrustworthyData
import net.corda.flows.ContractUpgradeFlow.Acceptor
import net.corda.flows.ContractUpgradeFlow.Instigator

/**
 * A flow to be used for upgrading state objects of an old contract to a new contract.
 *
 * The [Instigator] assembles the transaction for contract replacement and sends out change proposals to all participants
 * ([Acceptor]) of that state. If participants agree to the proposed change, they each sign the transaction.
 * Finally, [Instigator] sends the transaction containing all signatures back to each participant so they can record it and
 * use the new updated state for future transactions.
 */
object ContractUpgradeFlow {
    data class Proposal<OldState : ContractState, out NewState : ContractState>(override val stateRef: StateRef,
                                                                                override val modification: ContractUpgrade<OldState, NewState>,
                                                                                override val stx: SignedTransaction) : AbstractStateReplacementFlow.Proposal<ContractUpgrade<OldState, NewState>>

    private fun <OldState : ContractState, NewState : ContractState> assembleBareTx(stateRef: StateAndRef<OldState>,
                                                                                    contractUpgrade: ContractUpgrade<OldState, NewState>): TransactionBuilder {
        return TransactionType.General.Builder(stateRef.state.notary).apply {
            val (upgradedState, command) = contractUpgrade.upgrade(stateRef.state.data)
            withItems(stateRef, upgradedState, Command(command, stateRef.state.data.participants))
        }
    }

    class Instigator<OldState : ContractState, NewState : ContractState>(originalState: StateAndRef<OldState>,
                                                                         newContract: ContractUpgrade<OldState, NewState>) : AbstractStateReplacementFlow.Instigator<OldState, NewState, ContractUpgrade<OldState, NewState>>(originalState, newContract) {

        override fun assembleProposal(stateRef: StateRef, modification: ContractUpgrade<OldState, NewState>, stx: SignedTransaction) = Proposal(stateRef, modification, stx)

        override fun assembleTx(): Pair<SignedTransaction, Iterable<CompositeKey>> {
            return assembleBareTx(originalState, modification).let {
                it.signWith(serviceHub.legalIdentityKey)
                Pair(it.toSignedTransaction(false), originalState.state.data.participants)
            }
        }
    }

    class Acceptor(otherSide: Party) : AbstractStateReplacementFlow.Acceptor<ContractUpgrade<ContractState, ContractState>>(otherSide) {
        @Suspendable
        override fun verifyProposal(maybeProposal: UntrustworthyData<AbstractStateReplacementFlow.Proposal<ContractUpgrade<ContractState, ContractState>>>) = maybeProposal.unwrap { proposal ->
            val stx = serviceHub.storageService.validatedTransactions.getTransaction(proposal.stateRef.txhash) ?: throw IllegalStateException("We don't have a copy of the referenced state")
            val state = stx.tx.outRef<ContractState>(proposal.stateRef.index)
            val acceptedUpgrade = serviceHub.vaultService.getAcceptedContractStateUpgrade(state) ?: throw IllegalStateException("Contract state upgrade is unauthorised. State hash : ${state.ref}")
            val actualTx = proposal.stx.tx
            val expectedTx = assembleBareTx(state, proposal.modification).toWireTransaction()
            requireThat {
                "the proposed upgrade ${proposal.modification} is a trusted upgrade path" by (proposal.modification.javaClass == acceptedUpgrade.javaClass)
                "the proposed tx matches the expected tx for this upgrade" by (actualTx == expectedTx)
            }
            proposal
        }
    }
}
