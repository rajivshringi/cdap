package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.security.auth.Codec;
import com.continuuity.security.auth.FileBasedKeyManager;
import com.continuuity.security.auth.KeyIdentifier;
import com.continuuity.security.auth.KeyManager;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Guice bindings for FileBasedKeyManagers. This extends {@code SecurityModule} to provide
 * an instance of {@code FileBasedKeyManager}.
 */
public class FileBasedSecurityModule extends SecurityModule {

  @Override
  protected Provider<KeyManager> getKeyManagerProvider() {
    return new Provider<KeyManager>() {
      private CConfiguration cConf;
      private Codec<KeyIdentifier> keyIdentifierCodec;

      @Inject
      public void setCConfiguration(CConfiguration conf) {
        this.cConf = conf;
      }

      @Inject
      public void setKeyIdentifierCodec(Codec<KeyIdentifier> keyIdentifierCodec) {
        this.keyIdentifierCodec = keyIdentifierCodec;
      }

      @Override
      public KeyManager get() {
        FileBasedKeyManager keyManager = new FileBasedKeyManager(cConf, keyIdentifierCodec);
        try {
          keyManager.init();
        } catch (NoSuchAlgorithmException nsae) {
          throw Throwables.propagate(nsae);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
        return keyManager;
      }
    };
  }
}
