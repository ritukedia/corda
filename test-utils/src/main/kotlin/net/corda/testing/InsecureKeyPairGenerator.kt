package net.corda.testing

import net.i2p.crypto.eddsa.EdDSAPrivateKey
import net.i2p.crypto.eddsa.EdDSAPublicKey
import net.i2p.crypto.eddsa.spec.EdDSAGenParameterSpec
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable
import net.i2p.crypto.eddsa.spec.EdDSAPrivateKeySpec
import net.i2p.crypto.eddsa.spec.EdDSAPublicKeySpec
import java.security.KeyPair
import java.security.KeyPairGeneratorSpi
import java.security.SecureRandom
import java.util.*

/**
 * ED25519 key pair generator that allows non-secure random sources, DO NOT use outside of testing/simulation. Intended
 * to enable stable key generation for nodes.
 */
class InsecureKeyPairGenerator(private var random: Random) : KeyPairGeneratorSpi() {
    private val edParams = EdDSANamedCurveTable.getByName(EdDSAGenParameterSpec(EdDSANamedCurveTable.CURVE_ED25519_SHA512).name)

    override fun initialize(keysize: Int, newRandom: SecureRandom) {
        require(keysize == 256)
        this.random = newRandom
    }

    override fun generateKeyPair(): KeyPair {
        val seed = ByteArray(edParams.curve.field.getb() / 8)
        random.nextBytes(seed)

        val privKey = EdDSAPrivateKeySpec(seed, edParams)
        val pubKey = EdDSAPublicKeySpec(privKey.a, edParams)

        return KeyPair(EdDSAPublicKey(pubKey), EdDSAPrivateKey(privKey));
    }
}
