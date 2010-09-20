(defpackage :oci8-sugar
  (:use :oci8 :cl))

(in-package :oci8-sugar)

(defun sql-variables (statement)
  (let (vars)
    (cl-ppcre:do-scans (start end reg-start reg-end ":(\\w+)"
                              statement)
      (push (intern
             (string-upcase
              (subseq statement
                      (elt reg-start 0)
                      (elt reg-end 0))))
            vars))
    (nreverse vars)))

(defmacro define-sql-command (name (statement (&rest bind-sqlt-type-sizes)
                                              (&rest define-sqlt-type-sizes)))
  (alexandria:with-unique-names (stmthp binds sqlt type size tmp-func defs call-iteration)
    (let ((vars (sql-variables statement)))
      `(let ((,stmthp (create-statement ,statement))
             (,binds (make-array (length ',vars))))
         (declare (ignorable ,binds))
         ,@(loop for var in vars
              for sqlt-type-size in bind-sqlt-type-sizes
              for i from 1 collect
                `(destructuring-bind (,sqlt ,type &optional ,size)
                     ',sqlt-type-size
                   (setf (elt ,binds ,(1- i)) (bind-by-pos ,stmthp ,i ,type ,sqlt :size ,size))))
         (defun ,(if define-sqlt-type-sizes
                     tmp-func
                     name)
             ,vars
           ,@(loop for i from 1
                for var in vars collect
                  `(setf (pretty-data (elt ,binds ,(1- i))) ,var))
           (statement-execute ,stmthp *service-context* :iters 1 :mode :commit-on-success))
         ,(when define-sqlt-type-sizes
                `(let ((,defs (make-array (length ',define-sqlt-type-sizes))))
                   ,@(loop for define-sqlt-type-size in define-sqlt-type-sizes
                        for i from 1
                        collect
                          `(destructuring-bind (,sqlt ,type &optional ,size)
                               ',define-sqlt-type-size
                               (setf (elt ,defs ,(1- i))
                                     (define-by-pos ,stmthp ,i ,type ,sqlt :size ,size))))
                   (defun ,name ,(append vars '(&key) '(iterate-function))
                     (,tmp-func ,@vars)
                     (flet ((,call-iteration ()
                              (funcall iterate-function
                                       ,@(loop for i from 0 to (1- (length define-sqlt-type-sizes))
                                            collect
                                              `(pretty-data (elt ,defs ,i))))))
                       (handler-case
                           (loop do
                                (statement-fetch ,stmthp)
                                (,call-iteration))
                         (no-data () (,call-iteration)))))))))))
