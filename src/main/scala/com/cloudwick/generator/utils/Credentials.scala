package com.cloudwick.generator.utils

/**
 * Generic credentials for storing AWS Creds
 * @param accessKey AWS Access Key
 * @param secretKey AWS Secret Key
 * @param endPoint AWS endPoint for a service specified by
 *                 [[http://docs.aws.amazon.com/general/latest/gr/rande.html]]
 * @author ashrith
 */
case class Credentials(accessKey: String, secretKey: String, endPoint: String)
