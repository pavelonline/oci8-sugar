(defpackage :oci8-sugar
  (:use :oci8 :cl)
  (:export :define-sql-command
           :select-all
					 :with-clear-statements))

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

(defun columns-lengths (statement)
  (statement-execute statement *service-context* :mode :describe-only)
  (loop for i from 1 to (attr-get statement :attr-param-count :unsigned-short)
     for param = (param-get statement i)
     collect
       (attr-get param :attr-data-size :short)))

(defvar *prepared-statements* (make-hash-table))

(defclass statement ()
	((binds :reader binds :initform nil)
	 (defines :reader defines :initform nil)
	 (stmthp :reader handle :initform nil)))

(defmethod initialize-instance :after ((self statement)
																			 &key statement bind-sqlt-type-sizes
																			 define-sqlt-type-sizes
																			 &allow-other-keys)
	(with-slots (stmthp binds defines) self
		(setf stmthp (create-statement statement))
		(let ((vars (sql-variables statement)))
			(when vars
				(setf binds (make-array (length vars)))
				(loop for var in vars
						 for bind-record in bind-sqlt-type-sizes
						 for i from 1 do
						 (destructuring-bind (sqlt type &optional size)
								 bind-record
							 (setf (elt binds (1- i))
										 (bind-by-pos stmthp i type sqlt :size size))))))
		(when define-sqlt-type-sizes
			(let ((lengths (columns-lengths stmthp)))
				(setf defines (make-array (length define-sqlt-type-sizes)))
				(loop for i from 1
					 for def-record in define-sqlt-type-sizes do
						 (destructuring-bind (sqlt type &optional size)
								 def-record
							 (setf (elt defines (1- i))
										 (define-by-pos stmthp i type sqlt
																		:size
																		(case size
																			(:auto
																			 (nth (1- i) lengths))
																			(t size))))))))))

(defun get-statement (name)
	(or (gethash name *prepared-statements*)
			(setf (gethash name *prepared-statements*) (make-in

(defvar *statement-descriptions* (make-hash-table))
						 
(defmacro define-sql-command (name (statement (&rest bind-sqlt-type-sizes)
                                              (&rest define-sqlt-type-sizes)))
	(let ((vars (sql-variables statement)))
		(alexandria:with-unique-names (get-statement tmp-func stmt call-iteration)
			`(progn
				 (defun ,get-statement ()
					 (or (gethash ',name *prepared-statements*)
							 (setf (gethash ',name *prepared-statements*)
										 (make-instance 'statement
																		:statement ,statement
																		:define-sqlt-type-sizes ',define-sqlt-type-sizes
																		:bind-sqlt-type-sizes ',bind-sqlt-type-sizes))))
				 (defun ,(if define-sqlt-type-sizes
										 tmp-func
										 name)
						 ,vars
					 (let ((,stmt (,get-statement)))
						 (with-slots (binds) ,stmt
							 ,@(loop for i from 1
										for var in vars collect
											`(setf (pretty-data (elt binds ,(1- i))) ,var)))
						 (statement-execute (handle ,stmt) *service-context*
																:iters ,(if define-sqlt-type-sizes
																						0
																						1)
																:mode :commit-on-success)))
				 ,(when define-sqlt-type-sizes
								`(defun ,name ,vars
									 (,tmp-func ,vars)
									 (let ((,stmt (,get-statement)))
										 (with-slots (defines) ,stmt
											 (flet ((,call-iteration ()
																(funcall iterate-function
																				 ,@(loop for i from 0 to (1- (length define-sqlt-type-sizes))
																							collect
																								`(pretty-data (elt defines ,i))))))
												 (handler-case
														 (loop do
																	(statement-fetch (handle ,stmt))
																	(,call-iteration))
													 (no-data ()
														 (when (plusp (attr-get (handle ,stmt) :attr-rows-fetched :unsigned-char))
															 (,call-iteration)))))))))))))

(defmacro with-clear-statements (&body body)
	`(let ((*prepared-statements* nil))
		 ,@body))


(define-sql-command test-cur ("insert into pavel.test (id) values (:id)" ((:sqlt-int :int64)) ()))

(defmacro select-all (func-name &rest bind-args)
  (alexandria:with-unique-names (result)
    `(let ((,result))
       (,func-name ,@bind-args :iterate-function
                   (lambda (&rest data)
                     (push data
                           ,result)))
       (nreverse ,result))))
